"use strict";

/**
 * Prepare model sources if not defined, or when new state is detected.
 */
module.exports = {
  guard(m) {
    return !m.sourcesError && m.state.sources && m.state.sources.length > 0 && m.sourcesTs !== m.versionTs;
  },

  execute(m) {
    return m.state.sources.reduce((sources, src) => {
      if (src.sub_to_subject) {
        const sourceKey = src.sub_to_subject.replace(/\W/g, '_');
        const source = Object.assign({}, m.state.source_defaults, src);
        sources[sourceKey] = source; // NOTE: We do NOT subscribe to the error subject
        // if (source.error_subject) {
        //   sources[`${sourceKey}$error`] = Object.assign({}, source, {
        //     sub_to_subject: source.error_subject
        //   })
        // }
      }

      return sources;
    }, {});
  },

  assign(m, res, {
    logger
  }) {
    m.sourceKeys = Object.keys(res);
    m.sources = res;
    m.sourcesTs = m.versionTs;
    logger.info('Sources ready', {
      sourceKeys: m.sourceKeys
    });
  }

};