const mongoose = require('mongoose');
const Schema = mongoose.Schema;

const HistorySchema = new Schema(
  {
    downloadHistory: { type: Object },
    uf: { type: String },
    _id: { type: String },
    productsUsage: { type: Object },
  },
  { collection: 'History' }
);

const HistoryModel = mongoose.model('History', HistorySchema);
module.exports = HistoryModel;
