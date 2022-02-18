from sqlalchemy import create_engine
from sqlalchemy import types
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from PyQt5.QtCore import (
    QRunnable,
    QThread,
    QMetaObject,
    Qt,
    Q_ARG,
)


class UploadFileThread(QRunnable):
    def __init__(self, i, snow_con, con_details, database, schema, table, file_name, dataframe, all_text, dialog):
        QRunnable.__init__(self)
        self.i = i
        self.snow_con = snow_con
        self.con_details = con_details
        self.database = database
        self.schema = schema
        self.table = table
        self.file_name = file_name
        # self.if_exists_val = None
        self.engine = None
        self.df = dataframe
        self.all_text = all_text
        self.w = dialog

    def run(self):

        if_exists_val = None
        if self.i == 0:
            if_exists_val = 'replace'
        if self.i == 1:
            if_exists_val = 'append'
            self.all_text = False
        # if coming from main upload button
        if self.i == 99:
            if_exists_val = 'fail'

        try:
            # create empty table using sqlalchemy to_sql (to do: try with snow lib)
            self.engine = create_engine(f"snowflake://{self.con_details['account']}.snowflakecomputing.com",
                                        creator=lambda: self.snow_con)
            with self.engine.connect() as con:
                con.execute(f"USE DATABASE {self.database};")
                self.df.head(0).to_sql(schema=self.schema,
                                       name=self.table.lower(), con=con, if_exists=if_exists_val,
                                       index=False,
                                       dtype={col_name: types.TEXT for col_name in self.df}
                                       if self.all_text else None)

            # upload data using snowflake pandas function (snow_con instead of con!)
            for chunk in pd.read_csv(self.file_name, chunksize=100000, sep=None, engine="python"):
                chunk = chunk.convert_dtypes()
                success, nchunks, nrows, _ = write_pandas(conn=self.snow_con,
                                                          df=chunk,
                                                          database=self.database,
                                                          schema=self.schema,
                                                          table_name=self.table.upper())

        except ValueError:
            result = 'ValueError'
        except Exception as e:
            result = str(e)
        else:
            result = 'success'

        QMetaObject.invokeMethod(self.w, "upload_result",
                                 Qt.QueuedConnection,
                                 Q_ARG(str, result))