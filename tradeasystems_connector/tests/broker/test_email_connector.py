import unittest

from tradeasystems_connector.broker.email_connector import EmailConnector, getNotebookHTML, updateNotebook
from tradeasystems_connector.model.asset_type import AssetType
from tradeasystems_connector.model.broker import Broker
from tradeasystems_connector.model.currency import Currency
from tradeasystems_connector.model.instrument import Instrument
from tradeasystems_connector.model.order import Order
from tradeasystems_connector.model.order_type import OrderType
from tradeasystems_connector.model.side import Side
from tradeasystems_connector.tests import user_settings_tests


class EmailConnectorTestCase(unittest.TestCase):
    symbol = 'MSFT'
    broker = Broker.manual_email
    currency = Currency.usd
    asset_type = AssetType.equity
    instrument = Instrument(symbol, asset_type=asset_type, broker=broker, currency=currency)

    order = Order(instrument, 666, Side.buy, 10.66, order_type=OrderType.fill_or_kill)
    email_connector = EmailConnector(user_settings_tests)

    @unittest.skip  # no reason needed
    def test_send_buy_cancel(self):
        self.email_connector.sendOrder(self.order)
        # self.email_connector.cancelOrder(self.order)

    @unittest.skip  # no reason needed
    def test_send_html(self):
        text = ""
        html = """\
        <html>
          <head></head>
          <body>
            <p>Hi!<br>
               How are you?<br>
               Here is the <a href="http://www.python.org">link</a> you wanted.
            </p>
          </body>
        </html>
        """
        self.email_connector.__sendEmail__(recipient=user_settings_tests.email_notify, subject='test', body=text,
                                           html=html)

    @unittest.skip  # no reason needed
    def test_ipython_to_html(self):
        ipythonFile = 'test'
        htmlPythonFile = getNotebookHTML(ipythonFile)
        self.assertIsNotNone(htmlPythonFile)

    @unittest.skip  # no reason needed
    def test_ipython_exec(self):
        ipythonFile = 'test'
        result = updateNotebook(ipythonFile)
        self.assertIsNotNone(result)
        self.assertTrue(result)

    @unittest.skip  # no reason needed
    def test_sendNoteBook(self):
        ipythonFile = 'test'
        self.email_connector.sendNotebook(recipient=user_settings_tests.email_notify, notebookName=ipythonFile)
