bluebird = require "bluebird"
ftp = require "ftp"
csv_parse = require "csv-parse"
QuoteService = require "krumpled-api/api/services/QuoteService"

module.exports = do ->

  BATCH_SIZE = 10
  ZERO_CHAR = "0"
  MAX_RETRIES = 10

  log = (msg) -> process.stdout.write "[#{new Date()}] #{msg}\n"

  pad = (str, len, char) ->
    return "" unless str
    str = "#{str}"
    str = "#{char or ZERO_CHAR}#{str}" while str.length < len
    str

  printQuote = (quote) ->
    process.stdout.write "[#{quote.symbol}] #{quote.price}\n"

  processList = (list_name, do_save) ->
    [resolve, reject] = [null, null]
    symbols = null
    batch_index = 0
    batch_count = 0
    attempts = 0

    log "attempting to load #{list_name}"
    ftp_client = new ftp()

    failedFtpConnect = (err) ->
      ftp_client.destroy()

      log "failed connecting to ftp for #{list_name}"
      reject new Error err

    loadBatch = () ->
      start = batch_index * BATCH_SIZE
      end = start + BATCH_SIZE
      batch_symbols = symbols.slice start, end

      loadedBatch = (quotes) ->
        log "finished batch #{pad batch_index + 1, 3} for #{list_name}"

        if ++batch_index == batch_count - 1
          return false

        attempts = 0
        return loadBatch()

      failedBatch = ->
        if ++attempts >= MAX_RETRIES
          log "failed on batch #{batch_index + 1} too many times, skipping..."
          return ++batch_index and loadBatch()

        log "failed - retrying."
        loadBatch()

      QuoteService.lookup batch_symbols, null, do_save
        .then loadedBatch
        .catch failedBatch

    finishedAll = ->
      ftp_client.destroy()
      log "finished with #{list_name}"
      resolve true

    failedAll = ->
      ftp_client.destroy()
      reject new Error -1

    parsed = (err, data) ->
      if err
        log "#{err}"
        return reject new Error err

      symbols = (r[0] for r in (data.slice 1).slice 0, -1)
      batch_count = Math.ceil symbols.length / BATCH_SIZE
      log "loading #{list_name} symbols in batches of #{BATCH_SIZE}. total batches: #{batch_count}"

      loadBatch()
        .then finishedAll
        .catch failedAll

      true

    read = (err, result) ->
      if err
        ftp_client.destroy()
        log "failed readying list file..."
        return reject new Error err

      parser = csv_parse {
        delimiter: "|"
        relax: true
      }, parsed

      log "finished reading, piping to csv parser"
      result.pipe parser

    connected = ->
      log "conncected to ftp for #{list_name}, reading list file"
      ftp_client.get "/SymbolDirectory/#{list_name}.txt", read

    resolution = (fns...) ->
      [resolve, reject] = fns
      ftp_client.on "ready", connected
      ftp_client.on "error", failedFtpConnect

    ftp_client.connect
      host: "ftp.nasdaqtrader.com"

    new bluebird resolution

  getQuote = (stocks, do_save) ->
    finished = (quotes) ->
      printQuote q for q in quotes
      true

    QuoteService.lookup stocks, null, do_save
      .then finished

  getQuote.list = (list_name, do_save) ->
    processList list_name, do_save

  getQuote.all = (do_save) ->
    promise = {}
    start = new Date()

    diff = ->
      now = new Date().getTime()
      mili_diff = now - start.getTime()
      mili_diff / 1000

    finished = ->
      log "finished processing all lists in #{diff()} seconds"
      promise.resolve true

    errored = (err) ->
      log "failed processing all lists in #{diff()} seconds"
      log err
      promise.reject new Error "failed list"

    log "loading all symbols from lists, this may take a while...  saving?: #{do_save or false}"
  
    processAll = (resolve, reject) ->
      promise.resolve = resolve
      promise.reject = reject

      (bluebird.all [
        processList "nasdaqlisted", do_save
        processList "otherlisted", do_save
      ]).then finished
        .catch errored

      true

    new bluebird processAll

  getQuote
