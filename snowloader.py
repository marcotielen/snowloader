# pyinstaller --onefile --windowed --clean --add-data="main_stripped.ui;." --add-data="snowflake_instances.json;." --icon=snowflake.ico --add-data="snowflake.ico;." --splash="snowflake_splash.png" snowloader.py
# in hook-sqlalchemy.py add 'snowflake.sqlalchemy' to hiddenimports array for pyinstaller to pick up the snowflake dialect
from os import path
from PyQt5.QtWidgets import (
    QApplication,
    QDesktopWidget,
    QCheckBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QMainWindow,
    QMessageBox,
    QComboBox,
    qApp,
    QFileDialog,
    QCommandLinkButton,
    QPushButton,
    QAction,
    QDialog
)
from snowflake_connection import open_connection
import ui_def
from upload_file import UploadFileThread
from json import load
import sys
from io import StringIO
import webbrowser
import pandas as pd
import sqlalchemy
from table_model import PandasModel
from PyQt5.uic import loadUi
from spinner import QtWaitingSpinner
# add sqlalchemy.snowflake to the hook-sqlalchemy.py in the hiddenimports
# from sqlalchemy.dialects import registry
# registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
from PyQt5.QtCore import (
    QThreadPool,
    QThread,
    pyqtSlot,
    Qt,
)
from PyQt5.QtGui import QIcon


class Window(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowIcon(QIcon('snowflake.png'))

        # enable pysintaller to include files during build
        if getattr(sys, 'frozen', False):
            main_ui = path.join(sys._MEIPASS, 'main_stripped.ui')
            snf_inst = path.join(sys._MEIPASS, 'snowflake_instances.json')
        else:
            main_ui = path.join(sys.path[0], 'main_stripped.ui')
            snf_inst = path.join(sys.path[0], 'snowflake_instances.json')

        # load the UI; if statement needed for pyinstaller
        loadUi(main_ui, self)

        # load config file with locations
        with open(snf_inst, 'r') as snowflake_instances:
            self.snowflake_instances_dict = load(snowflake_instances)

        # create waiting spinner
        self.wspinner = QtWaitingSpinner(self.uploadTableView)
        self.wspinner.setRoundness(70.0)
        self.wspinner.setMinimumTrailOpacity(15.0)
        self.wspinner.setTrailFadePercentage(70.0)
        self.wspinner.setNumberOfLines(12)
        self.wspinner.setLineLength(10)
        self.wspinner.setLineWidth(5)
        self.wspinner.setInnerRadius(10)
        self.wspinner.setRevolutionsPerSecond(1)
        # self.wspinner.setColor(QColor(81, 4, 71))

        # declare for later use
        self.snow_con = None
        self.con_details = None
        self.df = None
        self.engine = None
        self.file_name = None
        self.link_button_advanced = False

        # connecting functions for GUI interactions
        self.connectionButton.clicked.connect(self.connection)
        self.changeSetLinkButton.clicked.connect(self.show_settings)
        self.selectUploadFileButton.clicked.connect(self.open_file_name_dialog)
        self.uploadFileButton.clicked.connect(lambda: self.upload_file(99))
        self.authenticatorComboBox.currentIndexChanged.connect(self.snowflake_password)
        self.environmentComboBox.currentIndexChanged.connect(self.account_name)
        self.databaseUploadComboBox.currentIndexChanged.connect(self.load_schemas)
        self.actionDocumentation.triggered.connect(self.open_help)

        self.environmentComboBox.clear()
        snf_envs = [snf_env['name'] for snf_env in self.snowflake_instances_dict['instances']]
        snf_envs.append("Other")
        self.environmentComboBox.addItems(snf_envs)
        self.account_name()

        # GUI elements to hide at start
        self.passwordLineEdit.setVisible(False)
        self.passwordLabel.setVisible(False)
        self.advancedSetGroupBox.setVisible(False)
        self.connectionDescLabel.setVisible(False)

        # allow sorting on preview table
        self.uploadTableView.setSortingEnabled(True)

    # open help section
    def open_help(self):
        webbrowser.open('https://www.github.com')

    # toggle between advanced and basic settings
    def show_settings(self):
        if not self.link_button_advanced:
            self.advancedSetGroupBox.setVisible(True)
            self.changeSetLinkButton.setText('Basic Settings')
            self.link_button_advanced = True
        elif self.link_button_advanced:
            self.advancedSetGroupBox.setVisible(False)
            self.changeSetLinkButton.setText('Advanced Settings')
            self.link_button_advanced = False

    # toggle visibility snowflake password on combobox change
    def snowflake_password(self):
        if self.authenticatorComboBox.currentText() == 'Snowflake': #currentIndix as alternative
            self.passwordLineEdit.setVisible(True)
            self.passwordLabel.setVisible(True)
        else:
            self.passwordLineEdit.setVisible(False)
            self.passwordLabel.setVisible(False)

    # Show account
    def account_name(self):
        if self.environmentComboBox.currentText() == 'Other':
            self.accountNameLineEdit.setVisible(True)
            self.accountNameLabel.setVisible(True)
        else:
            self.accountNameLineEdit.setVisible(False)
            self.accountNameLabel.setVisible(False)

    # setup connection and load databases to combobox
    def connection(self):

        if self.userLineEdit.text() == '':
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please enter a user id/e-mail first')
            msg.setWindowTitle("Error")
            msg.exec_()

        else:
            try:
                self.snow_con, self.con_details = open_connection(snow_envs=self.snowflake_instances_dict,
                                                                  snow_user=self.userLineEdit.text(),
                                                                  snow_account=self.environmentComboBox.currentText(),
                                                                  snow_authenticator=self.authenticatorComboBox.currentText(),
                                                                  snow_role=self.roleLineEdit.text(),
                                                                  snow_warehouse=self.warehouseLineEdit.text(),
                                                                  snow_password=self.passwordLineEdit.text(),
                                                                  snow_custom_account=self.accountNameLineEdit.text())

                # load databases to combobox on upload tab
                databases = self.snow_con.cursor().execute('show databases;')
                database_list = [row[1] for row in databases]
                database_list.insert(0,"")
                self.databaseUploadComboBox.clear()
                self.databaseUploadComboBox.addItems(database_list)
                self.connectionDescLabel.setVisible(True)

            except Exception as e:
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Critical)
                msg.setText(str(e))
                msg.setWindowTitle("Error")
                msg.exec_()


    # load all schemas
    def load_schemas(self):
        # load all schema's to combobox from selected database on upload tab
        current_db = self.databaseUploadComboBox.currentText()
        if current_db != '':
            schemas = self.snow_con.cursor().execute(f'select schema_name from {current_db}.information_schema.schemata order by schema_name;')
            schema_list = [row[0] for row in schemas]
            schema_list.insert(0, "")
            self.schemaUploadComboBox.clear()
            self.schemaUploadComboBox.addItems(schema_list)

    def open_file_name_dialog(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        self.file_name, _ = QFileDialog.getOpenFileName(self,
                                                        "QFileDialog.getOpenFileName()",
                                                        "",
                                                        "CSV Files (*.csv);;TEXT Files (*.txt);;All Files (*)",
                                                        options=options)
        if self.file_name:
            self.df = pd.read_csv(self.file_name, nrows=100000)
            self.df = self.df.convert_dtypes()
            model = PandasModel(self.df)
            # df.info(verbose=True)
            self.uploadTableView.setModel(model)
            # https: // github.com / idevloping / Data - Analyze - in -gui - Pyqt5 - python / blob / main / mainPyside.py

    def upload_file(self, i):

        if not self.snow_con:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please setup a connection first')
            msg.setWindowTitle("Error")
            msg.exec_()

        elif not self.file_name:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please select a file first')
            msg.setWindowTitle("Error")
            msg.exec_()

        else:
            # if not cancel
            if i != 2:
                # temporarily disable upload button during upload
                self.uploadFileButton.setDisabled(True)
                # start loading spinner
                self.wspinner.start()

                # start upload thread
                runnable = UploadFileThread(i, self.snow_con, self.con_details,self.databaseUploadComboBox.currentText(),
                                            self.schemaUploadComboBox.currentText(), self.tableUploadEdit.text(),
                                           self.file_name, self.df, self.allTextCheckBox.isChecked(), self)
                QThreadPool.globalInstance().start(runnable)

    # callback/slot for upload file thread
    @pyqtSlot(str)
    def upload_result(self, result):

        self.wspinner.stop()
        if result == 'success':
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Information)
            msg.setText("Upload Successful")
            msg.setWindowTitle("OK")
            msg.exec_()
        elif result == 'ValueError':
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Table already existent. Please choose:')
            msg.setWindowTitle("Warning")
            msg.addButton(QPushButton('Replace'), QMessageBox.YesRole)
            msg.addButton(QPushButton('Append'), QMessageBox.NoRole)
            msg.addButton(QPushButton('Cancel'), QMessageBox.RejectRole)
            ret_val = msg.exec_()
            self.upload_file(ret_val)
        else:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Critical)
            msg.setText(result)
            msg.setWindowTitle("Error")
            msg.exec_()

        # re-enable upload button; was disable during upload
        self.uploadFileButton.setDisabled(False)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = Window()
    try:
        # import in try statement as it can only be loaded at runtime after build
        import pyi_splash
        pyi_splash.close()
    except:
        pass
    window.show()
    sys.exit(app.exec_())