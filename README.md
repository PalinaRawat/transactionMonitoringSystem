Proposed use cases:

1) Large transactions
   - Check if the transaction amount exceeds a specified threshold (example: $10,000) for every transaction
2) Odd hour transactions
   - Check if a transaction occurs during unusual hours (example: between midnight and 5 am)
3) High frequency transactions
   - More than x (example: 10) transactions within y (example: 5 minutes) time by the same user
4) Inconsistent location
  - Flag transactions made by the same user at different locations (i.e, different merchants) within a short time window (example: within 1 hour)
5) Unusually large transactions
   - Group transactions by merchant. Flag transactions that are 10 times larger than the median transaction value at the same merchant
  
     
  
     
  
   
  
   
