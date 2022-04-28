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
    def __init__(self, if_exists_val, snow_con, con_details, database, schema, table, file_name, all_text, dialog, multi_index=None):
        QRunnable.__init__(self)
        self.snow_con = snow_con
        self.con_details = con_details
        self.database = database
        self.schema = schema
        self.table = table
        self.file_name = file_name
        self.engine = None
        self.all_text = all_text
        self.w = dialog
        self.if_exists_val = if_exists_val
        self.multi_index = multi_index

    def run(self):

        try:
            for i, chunk in enumerate(pd.read_csv(self.file_name, chunksize=100000, sep=None, engine="python", encoding="utf-8-sig")):

                chunk.columns = map(str.upper, chunk.columns)

                # if first chunk then create table
                if i == 0:
                    # first time only: create empty table using sqlalchemy to_sql (to do: try with snow lib)
                    self.engine = create_engine(f"snowflake://{self.con_details['account']}.snowflakecomputing.com",
                                                creator=lambda: self.snow_con)
                    with self.engine.connect() as con:
                        con.execute(f"USE DATABASE {self.database};")
                        chunk.head(0).to_sql(schema=self.schema,
                                             name=self.table.lower(),
                                             con=con,
                                             if_exists=self.if_exists_val.lower(),
                                             index=False,
                                             dtype={col_name: types.TEXT for col_name in chunk}
                                             if self.all_text else None)

                # upload data using snowflake pandas function (snow_con instead of con!)
                chunk = chunk.convert_dtypes()
                success, nchunks, nrows, _ = write_pandas(conn=self.snow_con,
                                                          df=chunk,
                                                          database=self.database,
                                                          schema=self.schema,
                                                          table_name=self.table.upper())

        # except ValueError as ve:
        #     result = ['ValueError', self.multi_index]
        # except Exception as e:
        #     result = [str(e), self.multi_index]
        except Exception as e:
            result = [e, self.multi_index]
            print(e.args)
        else:
            result = ['success', self.multi_index]

        QMetaObject.invokeMethod(self.w, "upload_result",
                                 Qt.QueuedConnection,
                                 Q_ARG(list, result))
