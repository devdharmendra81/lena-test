const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const AggregateHistorySchema = new Schema(
  {
    downloadHistory: { type: Object },
    uf: { type: String },
    _id: { type: String },
    productsUsage: { type: Object },
  },
  { collection: 'AggregateHistory' }
);

const AggregateHistoryModel = mongoose.model('AggregateHistory', AggregateHistorySchema);
module.exports = AggregateHistoryModel;
