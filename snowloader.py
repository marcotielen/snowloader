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
    QDialog,
    QTableView,
    QHeaderView
)
from snowflake_connection import open_connection
from upload_file import UploadFileThread
from json import load
import sys
import webbrowser
import pandas as pd
import itertools
import string
import sqlalchemy
# from dataframe_model import DataFrameModel
import table_model
from PyQt5.uic import loadUi
from spinner import QtWaitingSpinner
# add sqlalchemy.snowflake to the hook-sqlalchemy.py in the hiddenimports
from sqlalchemy.dialects import registry

registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
from PyQt5.QtCore import (
    QThreadPool,
    QThread,
    pyqtSlot,
    Qt,
    QModelIndex
)
from PyQt5.QtGui import QIcon, QPixmap
from pathlib import Path
# for windows icon
from ctypes import windll
# import logging
# logging.basicConfig(level=logging.DEBUG)
#
# import http.client
# http.client.HTTPConnection.debuglevel = 1


class Window(QMainWindow):
    def __init__(self):
        super().__init__()

        # enable pysintaller to include files during build
        if getattr(sys, 'frozen', False):
            main_ui = path.join(sys._MEIPASS, 'main.ui')
            snf_inst = path.join(sys._MEIPASS, 'snowflake_instances.json')
        else:
            main_ui = path.join(sys.path[0], 'main.ui')
            snf_inst = path.join(sys.path[0], 'snowflake_instances.json')
        # uncomment in case of including in build
        snf_inst = 'snowflake_instances.json'

        # load the UI; if statement needed for pyinstaller
        loadUi(main_ui, self)

        # load config file with locations
        try:
            with open(snf_inst, 'r') as snowflake_instances:
                self.snowflake_instances_dict = load(snowflake_instances)
        except FileNotFoundError:
            self.snowflake_instances_dict = None

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
        self.model = None
        self.database_list = list()
        self.queue_dict = dict()
        self.queue_counter_verifier = 0
        self.queue_multi_iteration_start = False

        # connecting functions for GUI interactions
        # menu bar
        self.actionDocumentation.triggered.connect(self.open_help)
        # connection
        self.connectionButton.clicked.connect(self.connection)
        self.changeSetLinkButton.clicked.connect(self.show_settings)
        self.authenticatorComboBox.currentIndexChanged.connect(self.snowflake_password)
        self.environmentComboBox.currentIndexChanged.connect(self.account_name)
        # upload
        self.databaseUploadComboBox.currentIndexChanged.connect(
            lambda: self.load_schemas(self.databaseUploadComboBox.currentText(), target=self.schemaUploadComboBox))
        self.uploadFileButton.clicked.connect(lambda: self.upload_file(self.actionUploadComboBox.currentIndex()))
        self.selectUploadFileButton.clicked.connect(self.open_file_name_dialog)
        # multi-upload
        self.selectUploadFolder.clicked.connect(self.select_folder_dialog)
        self.databaseMultiUploadComboBox.currentIndexChanged.connect(
            lambda: self.load_schemas(self.databaseMultiUploadComboBox.currentText(),
                                      target=self.schemaMultiUploadComboBox))
        self.applyMultiUploadButton.clicked.connect(self.apply_multi_changes)
        self.uniqueTableMultiUploadCheckBox.toggled.connect(self.toggle_multi_table_input)
        self.uploadFolderButton.clicked.connect(self.upload_folder)

        # col definitions multi upload data model
        self.multi_upload_cols = ['Index', 'Name', 'Size', 'Action', 'Database', 'Schema', 'Table Name', 'Status']

        # load connection options combobox
        self.environmentComboBox.clear()
        snf_envs = list()
        if self.snowflake_instances_dict is not None:
            snf_envs = [snf_env['name'] for snf_env in self.snowflake_instances_dict['instances']]
        snf_envs.append("Other")
        self.environmentComboBox.addItems(snf_envs)
        self.account_name()

        # GUI elements to hide at start
        self.passwordLineEdit.setVisible(False)
        self.passwordLabel.setVisible(False)
        self.advancedSetGroupBox.setVisible(False)
        self.connectionDescLabel.setVisible(False)

        # set combobox action options (multi-upload). Combobox values for single uplaod are defined in UI file
        self.action_list = ["", "Append", "Create/Replace and Append", "Ignore"]
        self.actionMultiUploadComboBox.addItems(self.action_list)

        # enable sorting on both tables
        self.uploadTableView.setSortingEnabled(True)
        self.multiUploadTableView.setSortingEnabled(True)

        # set update flag for auto-schema updates (allow for temporary disabling)
        self.update_flag = True

        self.showMaximized()

    # open help section
    def open_help(self):
        webbrowser.open('https://github.com/marcotielen/snowloader')

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
        if self.authenticatorComboBox.currentText() == 'Snowflake':  # currentIndix as alternative
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
                # create connection to Snowflake
                self.connectionDescLabel.setVisible(False)
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
                self.database_list = [row[1] for row in databases]
                self.database_list.insert(0, "")
                self.databaseUploadComboBox.clear()
                self.databaseUploadComboBox.addItems(self.database_list)

                # load databases to comboboxes on multi-upload tab
                self.databaseMultiUploadComboBox.clear()
                self.databaseMultiUploadComboBox.addItems(self.database_list)
                if self.model:
                    for row in range(self.model.rowCount(parent=QModelIndex())):
                        index = self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Database"))
                        self.multiUploadTableView.setIndexWidget(index,
                                                                 self.setting_combobox(index, self.database_list))

                # show successful connection; not by msgbox, because of multiple screens and external browser auth
                # opens the browser > msgbox could pop-up on another screen beneath the browser which is confusing
                self.connectionDescLabel.setVisible(True)

            except Exception as e:
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Critical)
                msg.setText(str(e))
                msg.setWindowTitle("Error")
                msg.exec_()

    # load all schemas
    def load_schemas(self, current_db, target=None, schema_target=None):
        if self.update_flag:
            if schema_target:
                col = schema_target.column()
                row = schema_target.row()
                target = self.findChild(QComboBox, f"combobox{col + 1}{row}")
            # load all schema's to combobox from selected database on upload tab
            if current_db != '':
                schemas = self.snow_con.cursor().execute(
                    f'select schema_name from {current_db}.information_schema.schemata order by schema_name;')
                schema_list = [row[0] for row in schemas]
                schema_list.insert(0, "")
                target.clear()
                target.addItems(schema_list)

    def open_file_name_dialog(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        self.file_name, _ = QFileDialog.getOpenFileName(self,
                                                        "QFileDialog.getOpenFileName()",
                                                        "",
                                                        "CSV Files (*.csv);;TEXT Files (*.txt);;All Files (*)",
                                                        options=options)
        if self.file_name:
            try:
                self.df = pd.read_csv(self.file_name, nrows=10000, sep=None, engine="python", encoding="utf-8-sig")
                self.df = self.df.convert_dtypes()

                # upload_model = table_model.DataFrameModel(self.df, editable=False)
                upload_model = table_model.Materials(self.df.values.tolist(),
                                                     headerdata=self.df.columns.values.tolist(), editable=False)
                # df.info(verbose=True)
                self.uploadTableView.setModel(upload_model)

                # resize table columns. note: automatic stretch option locks user resizing interaction
                for col in range(upload_model.columnCount(parent=QModelIndex())):
                    self.uploadTableView.horizontalHeader().resizeSection(col, int(
                        (self.uploadTableView.frameGeometry().width() - 60) / upload_model.columnCount(
                            parent=QModelIndex())))
                    self.uploadTableView.horizontalHeader().setStretchLastSection(True)

            except Exception as e:
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Critical)
                msg.setText(str(e))
                msg.setWindowTitle("Error")
                msg.exec_()

    def select_folder_dialog(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        dir_path = QFileDialog.getExistingDirectory(self, "Choose Directory", "", options=options)
        if dir_path:
            try:
                files_list = list()
                for count, p in enumerate(Path(dir_path).glob('*.csv')):
                    files_list.append(
                        [count, dir_path+"/"+p.name, f"{p.stat().st_size / float(1 << 20):,.1f} MB", "Append", "", "", "", ""])
                if len(files_list) > 0:
                    files_df = pd.DataFrame(files_list, columns=self.multi_upload_cols)

                    # self.model = table_model.DataFrameModel(files_df, editable=True)
                    self.model = table_model.Materials(files_df.values.tolist(),
                                                       headerdata=files_df.columns.values.tolist(), editable=True)
                    self.multiUploadTableView.setModel(self.model)
                    # hide index column
                    self.multiUploadTableView.setColumnHidden(self.multi_upload_cols.index("Index"), True)

                    # make cols read-only
                    delegate = table_model.ReadOnlyDelegate(self.multiUploadTableView)
                    self.multiUploadTableView.setItemDelegateForColumn(self.multi_upload_cols.index("Name"), delegate)
                    self.multiUploadTableView.setItemDelegateForColumn(self.multi_upload_cols.index("Size"), delegate)
                    # self.multiUploadTableView.setItemDelegateForColumn(self.multi_upload_cols.index("Status"), delegate)

                    # create action combobox

                    for row in range(self.model.rowCount(parent=QModelIndex())):
                        index = self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Action"))
                        self.multiUploadTableView.setIndexWidget(index, self.setting_combobox(index, self.action_list))

                    # create database combo
                    for row in range(self.model.rowCount(parent=QModelIndex())):
                        index = self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Database"))
                        self.multiUploadTableView.setIndexWidget(index,
                                                                 self.setting_combobox(index, self.database_list))
                    # create schema combo
                    for row in range(self.model.rowCount(parent=QModelIndex())):
                        index = self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Schema"))
                        self.multiUploadTableView.setIndexWidget(index, self.setting_combobox(index, []))

                    # resize table columns. note: automatic stretch option locks user resizing interaction
                    for col in range(self.model.columnCount(parent=QModelIndex())):
                        self.multiUploadTableView.horizontalHeader().resizeSection(col, int(
                            (self.multiUploadTableView.frameGeometry().width() - 20) / self.model.columnCount(
                                parent=QModelIndex())))
                        self.multiUploadTableView.horizontalHeader().setStretchLastSection(True)

                else:
                    msg = QMessageBox()
                    msg.setIcon(QMessageBox.Warning)
                    msg.setText("No csv files found")
                    msg.setWindowTitle("Error")
                    msg.exec_()

            except Exception as e:
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Critical)
                msg.setText(str(e))
                msg.setWindowTitle("Error")
                msg.exec_()

    # add combobox to table
    def setting_combobox(self, index, list):
        widget = QComboBox()
        widget.addItems(list)
        widget.setCurrentIndex(0)
        col = index.column()
        # row = index.row()
        row = self.multiUploadTableView.model().index(index.row(), self.multi_upload_cols.index("Index")).data()
        # set name for reference
        widget.setObjectName(f"combobox{col}{row}")
        # connect to setting data
        widget.currentTextChanged.connect(
            lambda value: self.model.setData(self.multiUploadTableView.model().index(row, col), value)
        )
        # connect to loading schema data; get row for target from index column of current row. combobox names are tied to index, not current row
        # optional: look into proxyfiltermodel option of pyqt5
        if index.column() == self.multi_upload_cols.index("Database"):
            widget.currentTextChanged.connect(
                lambda: self.load_schemas(widget.currentText(), schema_target=self.multiUploadTableView.model().index(
                    self.multiUploadTableView.model().index(index.row(), self.multi_upload_cols.index("Index")).data(),
                    col))
            )
            widget.currentTextChanged.connect(
                lambda: print(self.multiUploadTableView.model().index(index.row(), self.multi_upload_cols.index("Index")).data())
            )
        return widget

    # planned use; net yet active
    def setting_checkbox(self, index):
        widget = QCheckBox()
        widget.setChecked(True)
        widget.stateChanged.connect(
            lambda value: self.model.setData(index, value)
        )
        return widget

    def upload_file(self, i):

        if not self.snow_con:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please setup a connection')
            msg.setWindowTitle("Error")
            msg.exec_()

        elif not self.file_name:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please select a file')
            msg.setWindowTitle("Error")
            msg.exec_()

        elif i == 0:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please select an action')
            msg.setWindowTitle("Error")
            msg.exec_()

        else:
            # if cancel clicked on popup
            if i != 4:

                all_text = self.allTextCheckBox.isChecked()

                if_exists_val = None
                if i == 3:
                    if_exists_val = 'replace'
                if i == 2:
                    if_exists_val = 'append'
                    # in case of append don't use the all varchar option
                    all_text = False
                # if coming from main upload button
                if i == 1:
                    if_exists_val = 'fail'

                # temporarily disable upload button during upload
                self.uploadFileButton.setDisabled(True)
                # start loading spinner
                self.wspinner.start()

                # start upload thread
                runnable = UploadFileThread(if_exists_val,
                                            self.snow_con,
                                            self.con_details,
                                            self.databaseUploadComboBox.currentText(),
                                            self.schemaUploadComboBox.currentText(),
                                            self.tableUploadEdit.text(),
                                            self.file_name,
                                            all_text,
                                            self)
                QThreadPool.globalInstance().start(runnable)

    def upload_folder(self):

        if not self.snow_con:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please setup a connection first')
            msg.setWindowTitle("Error")
            msg.exec_()

        elif self.multiUploadTableView.model() is None or self.multiUploadTableView.model().columnCount(
                parent=QModelIndex()) == 0:
            msg = QMessageBox()
            msg.setIcon(QMessageBox.Warning)
            msg.setText('Please add items first')
            msg.setWindowTitle("Error")
            msg.exec_()

        else:
            # sort on db, schema and table name to be able to append
            df_multi_upload = pd.DataFrame(columns=self.multi_upload_cols)
            row_list = []
            for ele in range(self.multiUploadTableView.model().rowCount(parent=QModelIndex())):
                row = self.multiUploadTableView.model().index(ele, self.multi_upload_cols.index("Index")).data()
                for col in range(self.multiUploadTableView.model().columnCount(parent=QModelIndex())):
                    if col in [self.multi_upload_cols.index("Database"),
                                  self.multi_upload_cols.index("Schema")]:
                        row_list.append(self.findChild(QComboBox, f"combobox{col}{row}").currentText())
                    elif col == self.multi_upload_cols.index("Action"):
                        row_list.append(self.findChild(QComboBox, f"combobox{col}{row}").currentIndex())
                    else:
                        row_list.append(self.multiUploadTableView.model().index(row, col).data())

                df_multi_upload.loc[len(df_multi_upload)] = row_list
                row_list = []
            df_multi_upload["Table Name"] = df_multi_upload["Table Name"].str.upper()
            df_multi_upload = df_multi_upload.sort_values(['Database', 'Schema', 'Table Name'])
            print(df_multi_upload)

            # process all rows
            row_db = [None]
            row_schema = [None]
            row_table = [None]
            count=0
            total_rows = df_multi_upload.shape[0]
            dict_queue_id = 0
            dict_queue_id_append = 0
            self.queue_dict = dict()
            table_queue = dict()
            for ele_index, ele in df_multi_upload.iterrows():
                count += 1
                row = ele["Index"]
                row_action = ele["Action"]
                if_exists_val = None

                # if action is not set to ignore or empty
                if row_action == 0:
                    self.multiUploadTableView.model().setData(
                        self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Status")),
                        "No action set", Qt.EditRole)

                elif row_action == 3:
                    self.multiUploadTableView.model().setData(
                        self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Status")),
                        "Ignored", Qt.EditRole)
                else:

                    row_db.append(ele["Database"])
                    row_schema.append(ele["Schema"])
                    row_table.append(ele["Table Name"])

                    if row_db[1] == "":
                        self.multiUploadTableView.model().setData(
                            self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Status")),
                            "No database set", Qt.EditRole)
                    elif row_schema[1] == "":
                        self.multiUploadTableView.model().setData(
                            self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Status")),
                            "No schema set", Qt.EditRole)
                    elif row_table[1] == "":
                        self.multiUploadTableView.model().setData(
                            self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Status")),
                            "No table name set", Qt.EditRole)
                    else:
                        if_exists_val = None
                        if row_action == 1:
                            if_exists_val = 'append'
                            dict_queue_id += 1
                            dict_queue_id_append = 1
                        if row_action == 2:
                            # first time create else append
                            if row_db[0] == row_db[1] \
                                    and row_schema[0] == row_schema[1] \
                                    and row_table[0] == row_table[1]:
                                if_exists_val = 'append'
                                dict_queue_id_append += 1
                            else:
                                if_exists_val = 'replace'
                                dict_queue_id += 1
                                dict_queue_id_append = 1

                        # if_exists_val, snow_con, con_details, database, schema, table, file_name, all_text, dialog, multi_index=None, multi_last_item=False
                        # temporarily disable upload button during upload
                        self.uploadFolderButton.setDisabled(True)
                        # start loading spinner
                        # self.wspinner.start()

                        all_text = False
                        multi_index = self.multiUploadTableView.model().index(row, self.multi_upload_cols.index("Status"))
                        self.multiUploadTableView.model().setData(multi_index, "In Progress", Qt.EditRole)

                        ele_dict = dict()
                        ele_dict["if_exists_val"] = if_exists_val
                        ele_dict["snow_con"] = self.snow_con
                        ele_dict["con_details"] = self.con_details
                        ele_dict["database"] = ele["Database"]
                        ele_dict["schema"] = ele["Schema"]
                        ele_dict["table"] = ele["Table Name"]
                        ele_dict["file_name"] = ele["Name"]
                        ele_dict["all_text"] = all_text
                        ele_dict["dialog"] = self
                        ele_dict["multi_index"] = multi_index

                        if dict_queue_id_append == 1:
                            table_queue = dict()
                        table_queue[dict_queue_id_append] = ele_dict

                        # overwrite every time dict_queue_id is the same
                        self.queue_dict[dict_queue_id] = table_queue

                    # pop first item from list; needed for row over row comparison
                    row_db.pop(0)
                    row_schema.pop(0)
                    row_table.pop(0)

            # empty counter; needed to determine whether all first (per target table) files have been processed
            self.queue_counter_verifier = 0
            # start all first files per target table
            for i in self.queue_dict.keys():
                for key, value in self.queue_dict[i].items():
                    if key == 1:
                        runnable = UploadFileThread(**value)
                        QThreadPool.globalInstance().start(runnable)

    # function for the apply changes button on multi upload tab
    def apply_multi_changes(self):
        rows = []
        # Either all rows or only selection
        if self.appendAllMultiUploadRadio.isChecked():
            rows = [i for i in range(self.multiUploadTableView.model().rowCount(parent=QModelIndex()))]
        if self.applySelectionMultiUploadRadio.isChecked():
            rows = sorted(set(index.row() for index in
                              self.multiUploadTableView.selectedIndexes()))

        # initiate name generator in case auto-generated checkbox is checked
        gen = suffix_generator()
        # iterate through all rows
        for row in rows:
            # update action
            if self.actionMultiUploadCheckBox.isChecked():
                all_items = [self.actionMultiUploadComboBox.itemText(i) for i in
                             range(self.actionMultiUploadComboBox.count())]
                target = self.findChild(QComboBox, f"combobox{self.multi_upload_cols.index('Action')}{row}")
                # load all items
                target.clear()
                target.addItems(all_items)
                # set value
                target.setCurrentText(self.actionMultiUploadComboBox.currentText())
            # update database and schema
            if self.schemaMultiUploadCheckBox.isChecked():
                all_schema_items = [self.schemaMultiUploadComboBox.itemText(i) for i in
                                    range(self.schemaMultiUploadComboBox.count())]
                db_target = self.findChild(QComboBox, f"combobox{self.multi_upload_cols.index('Database')}{row}")
                schema_target = self.findChild(QComboBox, f"combobox{self.multi_upload_cols.index('Schema')}{row}")

                # set db and schema values
                # temporarily disconnect combobox action to prevent many schema queries. Load from apply section
                self.update_flag = False
                db_target.setCurrentText(self.databaseMultiUploadComboBox.currentText())
                self.update_flag = True
                schema_target.clear()
                schema_target.addItems(all_schema_items)
                schema_target.setCurrentText(self.schemaMultiUploadComboBox.currentText())
            # set table name values
            if self.tableMultiUploadCheckBox.isChecked():
                index = self.multiUploadTableView.model().index(row, self.multi_upload_cols.index('Table Name'))
                # generate table names: sanitized filename with suffix
                if self.uniqueTableMultiUploadCheckBox.isChecked():
                    filename_san = "".join([ch for ch in self.multiUploadTableView.model().index(row,
                                                                                                 self.multi_upload_cols.index(
                                                                                                     'Name')).data() if
                                            ch.isalpha() or ch=="_"])
                    self.multiUploadTableView.model().setData(index, filename_san.upper() + '_' + next(gen),
                                                              Qt.EditRole)
                # copy table name from line edit
                else:
                    self.multiUploadTableView.model().setData(index, self.tableMultiUploadEdit.text(), Qt.EditRole)

    # callback/slot for upload file thread
    @pyqtSlot(list)
    def upload_result(self, result):
        # if single upload
        if result[1] is None:
            self.wspinner.stop()
            if result[0] == 'success':
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Information)
                msg.setText("Upload Successful")
                msg.setWindowTitle("OK")
                msg.exec_()
            elif type(result[0]).__name__ == 'ValueError' and \
                str(result[0]).startswith('Table') and \
                str(result[0]).endswith('already exists.'):
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Warning)
                msg.setText('Table already existent. Please choose:')
                msg.setWindowTitle("Warning")
                msg.addButton(QPushButton('Append'), QMessageBox.YesRole)
                msg.addButton(QPushButton('Replace'), QMessageBox.NoRole)
                msg.addButton(QPushButton('Cancel'), QMessageBox.RejectRole)
                ret_val = int(msg.exec_())
                self.upload_file(ret_val+2)
            else:
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Critical)
                msg.setText(str(result[0]))
                msg.setWindowTitle("Error")
                msg.exec_()
        else:
            # set progress in status column
            self.multiUploadTableView.model().setData(result[1], str(result[0]), Qt.EditRole)
            # allow app to update progress
            QApplication.processEvents()
            self.queue_counter_verifier += 1
            # process all items beyond the first per target table when those have all finished
            if len(self.queue_dict.keys()) == self.queue_counter_verifier:
                for i in self.queue_dict.keys():
                    for key, value in self.queue_dict[i].items():
                        if key != 1:
                            runnable = UploadFileThread(**value)
                            QThreadPool.globalInstance().start(runnable)
                            self.queue_counter_verifier += 1

        # re-enable upload button; was disable during upload
        self.uploadFileButton.setDisabled(False)
        # if self.queue_counter += 1
        self.uploadFolderButton.setDisabled(False)

    # if multi upload
    def toggle_multi_table_input(self):
        if self.uniqueTableMultiUploadCheckBox.isChecked():
            self.tableMultiUploadEdit.setDisabled(True)
        else:
            self.tableMultiUploadEdit.setDisabled(False)


# table name suffix generator for table names suffix (A, B, C, ..., AA, AB)
def suffix_generator():
    for i in itertools.count(1):
        for p in itertools.product(string.ascii_uppercase, repeat=i):
            yield ''.join(p)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    my_app_id = u'opensource.snowloader.main.1'  # arbitrary string

    # windows will show python icon on taskbar if if not registered as separate app
    windll.shell32.SetCurrentProcessExplicitAppUserModelID(my_app_id)
    window = Window()

    icon = QIcon()
    icon.addPixmap(QPixmap('snowflake.ico'), QIcon.Selected, QIcon.On)
    window.setWindowIcon(icon)
    # window.setWindowIcon(QIcon('snowflake.png'))

    # show splash screen
    try:
        # import in try statement as it can only be loaded at runtime after build
        import pyi_splash

        pyi_splash.close()
    except:
        pass

    window.show()
    sys.exit(app.exec_())