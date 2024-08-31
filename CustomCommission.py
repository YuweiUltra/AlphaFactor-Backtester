from zipline.api import commission



class PercentageCommissionModel(commission.CommissionModel):
    def __init__(self, cost=0.001):
        self.cost = cost

    def calculate(self, order, transaction):
        # Apply commission based on percentage of the transaction value
        return transaction.price * transaction.amount * self.cost
