/**
 * Process an individual message.
 */

const methods = require('./methods')

async function processItem({ data, dataObj, msgSeq }, ctx) {
  const { logger, subSubject } = ctx
  try {
    /*
      Validate build method.
     */

    if (!dataObj.method) throw new Error('Build method undefined')

    const method = methods[dataObj.method]

    if (!method) throw new Error('Build method not supported')

    /*
      Invoke build method.
     */

    const startedAt = new Date()
    const buildRes = await method(dataObj, ctx)
    const finishedAt = new Date()

    if (!buildRes) throw new Error('Build result undefined')

    logger.info('Built', {
      buildRes,
      msgSeq,
      subSubject,
      startedAt,
      finishedAt
    })
  } catch (err) {
    logger.error('Processing error', { msgSeq, subSubject, err, dataObj })
  }
}

module.exports = processItem
