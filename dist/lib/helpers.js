"use strict";

/**
 * Method helpers.
 */
async function getAuthUser({
  authenticate,
  logger,
  passport,
  userService
}) {
  const accessToken = await passport.getJWT();
  let user;

  if (accessToken) {
    try {
      const payload = await passport.verifyJWT(accessToken);
      user = await userService.get(payload.userId);
    } catch (err) {
      logger.error('Get user error', {
        err
      });
    }
  }

  if (!user) {
    logger.info('Authenticating');
    const authRes = await authenticate();
    const payload = await passport.verifyJWT(authRes.accessToken);
    user = await userService.get(payload.userId);
  }

  logger.info('Authenticated as', {
    user
  });
  return user;
}

module.exports = {
  getAuthUser
};