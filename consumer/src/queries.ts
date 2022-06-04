export function getInsertQuery({
  amount,
  nameOrig,
  oldbalanceOrg,
  newbalanceOrig,
  nameDest,
  oldbalanceDest,
  newbalanceDest,
  isFraud,
  transaction_date,
}) {
  return `
  BEGIN BATCH
  INSERT INTO "topTransactions" (
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest") VALUES  (
  ${amount},
  '${nameOrig}',
  ${oldbalanceOrg},
  ${newbalanceOrig},
  '${nameDest}',
  ${oldbalanceDest},
  ${newbalanceDest});
  INSERT INTO "fraudTransactions" (
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud") VALUES  (
  ${amount},
  '${nameOrig}',
  ${oldbalanceOrg},
  ${newbalanceOrig},
  '${nameDest}',
  ${oldbalanceDest},
  ${newbalanceDest},
  ${!!isFraud});
  INSERT INTO "incomingTransactions" (
    "amount",
    "nameDest",
    "transaction_date" ) VALUES  (
  ${amount},
  '${nameDest}',
  '${transaction_date}');
  APPLY BATCH`
}
