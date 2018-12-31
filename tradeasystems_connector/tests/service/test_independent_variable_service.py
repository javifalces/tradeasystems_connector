import datetime
import unittest

from tradeasystems_connector.model.independent_variable import IndependentVariable
from tradeasystems_connector.model.ratio import Ratio
from tradeasystems_connector.service.independent_variable_service import IndependentVariableService
from tradeasystems_connector.tests import user_settings_tests


class IndependentVariableServiceTestCase(unittest.TestCase):
    user_settings = user_settings_tests
    independent_variable_service = IndependentVariableService(user_settings)

    fromDate = datetime.datetime(year=2017, day=1, month=1)
    toDate = datetime.datetime(year=2018, day=26, month=6)

    def test_getEURUSD(self):
        # clean first
        fromDate = datetime.datetime(year=2004, day=3, month=5)
        data = self.independent_variable_service.getVariableData(
            IndependentVariable.prices_eur_usd,
            fromDate=fromDate,
            toDate=self.toDate,
            force_download=True
        )

        self.assertIsNotNone(data)
        self.assertEqual(len(data.columns), 1)
        self.assertEqual((data.columns[0]), Ratio.ratio)
        self.assertEqual(fromDate, (data.index[0]))

    def test_getVIX(self):
        # clean first

        data = self.independent_variable_service.getVariableData(
            IndependentVariable.prices_vix,
            fromDate=self.fromDate,
            toDate=self.toDate,
            force_download=True
        )

        self.assertIsNotNone(data)
        self.assertEqual(len(data.columns), 1)
        self.assertEqual((data.columns[0]), Ratio.ratio)

    def test_getVariables(self):
        # clean first
        independentVarList = [IndependentVariable.prices_vix,
                              IndependentVariable.macro_interest_rates_us,
                              IndependentVariable.prices_sp500,
                              IndependentVariable.prices_dax,
                              IndependentVariable.prices_eur_usd
                              ]
        fromDate = datetime.datetime(year=2004, day=3, month=1)
        for independentVar in independentVarList:
            data = self.independent_variable_service.getVariableData(
                independentVar,
                fromDate=fromDate,
                toDate=self.toDate,
                force_download=True
            )

            self.assertIsNotNone(data)
            self.assertEqual(len(data.columns), 1)
            self.assertEqual((data.columns[0]), Ratio.ratio)
            # self.assertEqual(fromDate, (data.index[0]))
