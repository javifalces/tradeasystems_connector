import os
from pathlib import Path

from tradeasystems_connector.broker.broker_connector import BrokerConnector
from tradeasystems_connector.conf.log_settings import logger


def getNotebookHTML(python_notebookFile):
    # http://blog.fperez.org/2012/09/blogging-with-ipython-notebook.html
    # nbconvert -f blogger-html python_notebookFile
    import os
    notebookfile = '%s.ipynb' % python_notebookFile

    if os.path.isfile(notebookfile):
        command = 'jupyter nbconvert --to html  %s' % notebookfile
        os.system(command)
        # read html
        htmlFile = python_notebookFile + '.html'
        if os.path.isfile(htmlFile):
            output = open(htmlFile, 'r').read()
            os.remove(htmlFile)
            return output
        else:
            print('%s not found ' % htmlFile)
            return None
    else:
        print('%s not found ' % notebookfile)
        return None


def updateNotebook(python_notebookFile):
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor
    notebookfile = '%s.ipynb' % python_notebookFile

    if os.path.isfile(notebookfile):

        try:
            with open(notebookfile) as f:
                # run
                nb = nbformat.read(f, as_version=4)
                # execute
                ep = ExecutePreprocessor(timeout=600, kernel_name='python3')
                currentPath = Path(__file__).parent
                pathRoot = currentPath.parent.parent
                ep.preprocess(nb, {'metadata': {'path': str(pathRoot)}})
                # save
            with open(notebookfile, 'wt') as f:
                nbformat.write(nb, f)
            return True
        except Exception as e:
            print('Error executing notebook %s : %s' % (notebookfile, str(e)))
            return False
    else:
        print('%s doesnt exist!' % notebookfile)
    return False


class EmailConnector(BrokerConnector):
    user_settings = None
    notification = None
    idCounter = 0

    def __init__(self, user_settings):
        self.user_settings = user_settings
        self.notification = self.user_settings.email_notify

    def sendNotebook(self, recipient, notebookName):
        if updateNotebook(notebookName):
            htmlContent = getNotebookHTML(notebookName)
            self.__sendEmail__(recipient=recipient, subject=notebookName, body='', html=htmlContent)

    def __sendEmail__(self, recipient, subject, body, html=None, fileToSendArray=[]):
        import smtplib
        import mimetypes
        from email.mime.multipart import MIMEMultipart
        from email import encoders
        from email.mime.audio import MIMEAudio
        from email.mime.base import MIMEBase
        from email.mime.image import MIMEImage
        from email.mime.text import MIMEText

        fromEmail = self.user_settings.email_address
        try:

            msg = MIMEMultipart()
            msg['From'] = fromEmail
            msg['To'] = recipient
            msg['Subject'] = subject
            body = body
            msg.attach(MIMEText(body, 'plain'))
            if html is not None and isinstance(html, str):
                msg.attach(MIMEText(html, 'html'))

            # %% Atachemnt
            if fileToSendArray is not None and len(fileToSendArray) > 0:
                for fileToSend in fileToSendArray:
                    if fileToSend is not None and os.path.isfile(fileToSend):
                        logger.debug('adding file ' + fileToSend)

                        ctype, encoding = mimetypes.guess_type(fileToSend)
                        if ctype is None or encoding is not None:
                            ctype = "application/octet-stream"

                        maintype, subtype = ctype.split("/", 1)

                        if maintype == "text":
                            fp = open(fileToSend)
                            # Note: we should handle calculating the charset
                            attachment = MIMEText(fp.read(), _subtype=subtype)
                            fp.close()
                        elif maintype == "image":
                            fp = open(fileToSend, "rb")
                            attachment = MIMEImage(fp.read(), _subtype=subtype)
                            fp.close()
                        elif maintype == "audio":
                            fp = open(fileToSend, "rb")
                            attachment = MIMEAudio(fp.read(), _subtype=subtype)
                            fp.close()
                        else:
                            fp = open(fileToSend, "rb")
                            attachment = MIMEBase(maintype, subtype)
                            attachment.set_payload(fp.read())
                            fp.close()
                            encoders.encode_base64(attachment)
                        attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
                        msg.attach(attachment)
            result = False
            counter = 3
            while (not result and counter > 0):
                try:
                    server = smtplib.SMTP(self.user_settings.email_smtp_host, self.user_settings.email_smtp_port)
                    server.ehlo()
                    server.starttls()
                    server.ehlo()

                    server.login(fromEmail, self.user_settings.email_password)
                    text = msg.as_string()
                    problems = server.sendmail(fromEmail, recipient, text)
                    server.quit()
                    result = True
                except Exception as e:
                    logger.error("Error: unable to send email retry[%d] :%s" % (counter, str(e)))
                    result = False
                    counter -= 1
                    os.sleep(5)
            if result:
                logger.info("Successfully sent email")
            else:
                logger.error("Error: unable to send email")

        except:
            logger.error("Error: unable to send email")

    def sendOrder(self, order):
        bodyString = "%s in %s_%s  [%s]  %i@%.3f" % (
            order.side, order.instrument.symbol, order.instrument.currency, order.order_type, order.volume, order.price)
        self.__sendEmail__(recipient=self.notification, subject=bodyString, body=bodyString)
        order.unique_id = self.idCounter
        self.idCounter += 1
        return order

    def cancelOrder(self, order):
        bodyString = "cancel %s in %s_%s  [%s]  %i@%.3f" % (
            order.side, order.instrument.symbol, order.instrument.currency, order.order_type, order.volume, order.price)
        self.__sendEmail__(recipient=self.notification, subject=bodyString, body=bodyString)

    def getAccountBalance(self, broker):
        pass
