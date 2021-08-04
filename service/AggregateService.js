const _ = require('lodash');

function aggregateHistory(db) {
  db.collection("History").find({}).toArray((err, histories) => {
    if(err) {
      console.log('Error fetching History: ', err);
      return;
    }

    if(_.isEmpty(histories)) {
      console.log('No history records to aggregate');
      return;
    }
    const aggregatedHistoryArray = getAggregatedHistory(histories);

    db.collection("AggregateHistory").insertMany(aggregatedHistoryArray, (err) => {
      if(err) {
        console.log('Error saving Aggregate history: ', err);
        return;
      }

      console.log('Aggregate history records saved successfully');
      return;
    });
  });
}

module.exports = {
  aggregateHistory,
};

function getAggregatedHistory (histories) {
  histories = histories.sort((a, b) => {
    return Object.keys(a.downloadHistory).length > Object.keys(b.downloadHistory).length? -1:
      Object.keys(a.downloadHistory).length < Object.keys(b.downloadHistory).length ?1: 0;
  })
  const aggregatedHistory = histories.reduce((acc, history) => {
    history._id = new Date().getTime().toString();
    const existHistory = acc.find(({uf}) => uf === history.uf);
    if(_.isEmpty(existHistory)) {
      acc.push(history);
      return acc;
    }

    if(!history.downloadHistory) {
      return acc;
    }

    //determine downloadHistory
    Object.keys(history.downloadHistory).forEach((downloadHistoryKey) => {
      // determine firstDownloadTS
      const firstDownloadTS = history.downloadHistory[downloadHistoryKey].firstDownloadTS;
      const existingFirstDownloadTS = _.get(existHistory, `downloadHistory.${downloadHistoryKey}.firstDownloadTS`, null);
      if(
        firstDownloadTS &&
        existingFirstDownloadTS &&
        new Date(firstDownloadTS) < new Date(existingFirstDownloadTS)
      ) {
        existHistory.downloadHistory[downloadHistoryKey].firstDownloadTS = firstDownloadTS;
      }
      // determine hasDownloaded
      const hasDownloaded = history.downloadHistory[downloadHistoryKey].hasDownloaded;
      const existingHasDownloaded = _.get(existHistory, `downloadHistory.${downloadHistoryKey}.hasDownloaded`, undefined);
      if(
        !_.isUndefined(hasDownloaded) &&
        !_.isUndefined(existingHasDownloaded)
      ) {
        existHistory.downloadHistory[downloadHistoryKey].hasDownloaded = hasDownloaded || existingHasDownloaded;
      }
      // determine latestDownloadTS
      const latestDownloadTS = history.downloadHistory[downloadHistoryKey].latestDownloadTS;
      const existingLatestDownloadTS = _.get(existHistory, `downloadHistory.${downloadHistoryKey}.latestDownloadTS`, null);
      if(
        latestDownloadTS &&
        existingLatestDownloadTS &&
        new Date(latestDownloadTS) > new Date(existingLatestDownloadTS)
      ) {
        existHistory.downloadHistory[downloadHistoryKey].latestDownloadTS = latestDownloadTS;
      }
      // determine downloadCount
      const downloadCount = history.downloadHistory[downloadHistoryKey].downloadCount;
      const existingDownloadCount = _.get(existHistory, `downloadHistory.${downloadHistoryKey}.downloadCount`, null);
      if(
        downloadCount &&
        existingDownloadCount &&
        downloadCount > existingDownloadCount
      ) {
        existHistory.downloadHistory[downloadHistoryKey].downloadCount = downloadCount;
      }

    });

    const appLaunch = _.get(history, 'productsUsage.hist.appLaunch', null);
    const existAppLaunch = _.get(existHistory, 'productsUsage.hist.appLaunch', {});
    if(!appLaunch) {
      return acc;
    }

    //determine productsUsage.hist.appLaunch
    Object.keys(appLaunch).forEach((appLaunchKey) => {
      if(_.isEmpty(appLaunch[appLaunchKey])) {
        return;
      }

      Object.keys(appLaunch[appLaunchKey]).forEach((innerAppLaunchKey) => {
        const innerAppLaunchValue = appLaunch[appLaunchKey][innerAppLaunchKey];
        const existingInnerAppLaunchValue = existAppLaunch? _.get(existAppLaunch, `${appLaunchKey}.${innerAppLaunchKey}`, null): null;
        if(
          !existingInnerAppLaunchValue &&
          innerAppLaunchValue
        ) {
          if(!existAppLaunch[appLaunchKey]) {
            existAppLaunch[appLaunchKey] = {[innerAppLaunchKey]: innerAppLaunchValue};
          } else {
            existAppLaunch[appLaunchKey][innerAppLaunchKey] = innerAppLaunchValue;
          }
          return;
        }
        if(
          existingInnerAppLaunchValue &&
          innerAppLaunchValue
        ) {
          existAppLaunch[appLaunchKey][innerAppLaunchKey] +=  innerAppLaunchValue;
          return;
        }

      });

    });

    //.
    const usageAppLaunch = _.get(history, 'productsUsage.appLaunch', null);
    const existUsageAppLaunch = _.get(existHistory, 'productsUsage.appLaunch', {});
    if(!usageAppLaunch) {
      return acc;
    }

    //determine productsUsage.appLaunch
    Object.keys(usageAppLaunch).forEach((appLaunchKey) => {
      if(_.isEmpty(usageAppLaunch[appLaunchKey])) {
        return;
      }

      // determine firstLaunchTS
      const firstLaunchTS = usageAppLaunch[appLaunchKey].firstLaunchTS;
      const existingFirstLaunchTS = _.get(existUsageAppLaunch, `${appLaunchKey}.firstLaunchTS`, null);
      if(
        firstLaunchTS &&
        existingFirstLaunchTS &&
        new Date(firstLaunchTS) < new Date(existingFirstLaunchTS)
      ) {
        existUsageAppLaunch[appLaunchKey].firstLaunchTS = firstLaunchTS;
      }
      // determine launchCount
      const launchCount =  usageAppLaunch[appLaunchKey].launchCount;
      const existingLaunchCount = _.get(existUsageAppLaunch, `${appLaunchKey}.launchCount`, null);
      if(
        !_.isUndefined(launchCount) &&
        !_.isUndefined(existingLaunchCount)
      ) {
        existUsageAppLaunch[appLaunchKey].launchCount += launchCount;
      }
      // determine latestLaunchTS
      const latestLaunchTS = usageAppLaunch[appLaunchKey].latestLaunchTS;
      const existingLatestLaunchTS = _.get(existUsageAppLaunch, `${appLaunchKey}.latestLaunchTS`, null);
      if(
        latestLaunchTS &&
        existingLatestLaunchTS &&
        new Date(latestLaunchTS) > new Date(existingLatestLaunchTS)
      ) {
        existUsageAppLaunch[appLaunchKey].latestLaunchTS = latestLaunchTS;
      }

    });

    return acc;
  }, []);

  return aggregatedHistory;
}
