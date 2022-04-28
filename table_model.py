# from PyQt5 import QtCore
# from PyQt5.QtWidgets import QComboBox, QCheckBox
# from PyQt5.QtWidgets import QStyledItemDelegate, QItemDelegate
# import pandas as pd
#
#
# class DataFrameModel(QtCore.QAbstractTableModel):
#     DtypeRole = QtCore.Qt.UserRole + 1000
#     ValueRole = QtCore.Qt.UserRole + 1001
#
#     def __init__(self, df=pd.DataFrame(), parent=None, editable=False):
#         super(DataFrameModel, self).__init__(parent)
#         self._dataframe = df
#         self._editable = editable
#
#     def setDataFrame(self, dataframe):
#         self.beginResetModel()
#         self._dataframe = dataframe.copy()
#         self.endResetModel()
#
#     def dataFrame(self):
#         return self._dataframe
#
#     dataFrame = QtCore.pyqtProperty(pd.DataFrame, fget=dataFrame, fset=setDataFrame)
#
#     @QtCore.pyqtSlot(int, QtCore.Qt.Orientation, result=str)
#     def headerData(self, section: int, orientation: QtCore.Qt.Orientation, role: int = QtCore.Qt.DisplayRole):
#         if role == QtCore.Qt.DisplayRole:
#             if orientation == QtCore.Qt.Horizontal:
#                 return self._dataframe.columns[section]
#             else:
#                 return str(self._dataframe.index[section])
#         return QtCore.QVariant()
#
#     def rowCount(self, parent=QtCore.QModelIndex()):
#         if parent.isValid():
#             return 0
#         return len(self._dataframe.index)
#
#     def columnCount(self, parent=QtCore.QModelIndex()):
#         if parent.isValid():
#             return 0
#         return self._dataframe.columns.size
#
#     def data(self, index, role=QtCore.Qt.DisplayRole):
#         if not index.isValid() or not (0 <= index.row() < self.rowCount() \
#                                        and 0 <= index.column() < self.columnCount()):
#             return QtCore.QVariant()
#         row = self._dataframe.index[index.row()]
#         col = self._dataframe.columns[index.column()]
#         dt = self._dataframe[col].dtype
#
#         val = self._dataframe.iloc[row][col]
#         if role == QtCore.Qt.DisplayRole:
#             return str(val)
#         elif role == DataFrameModel.ValueRole:
#             return val
#         if role == DataFrameModel.DtypeRole:
#             return dt
#         return QtCore.QVariant()
#
#     def roleNames(self):
#         roles = {
#             QtCore.Qt.DisplayRole: b'display',
#             DataFrameModel.DtypeRole: b'dtype',
#             DataFrameModel.ValueRole: b'value'
#         }
#         return roles
#
#     def flags(self, index):
#         if self._editable == True:
#             return QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsEditable | QtCore.Qt.ItemIsUserCheckable
#         else:
#             return QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEnabled
#
#     # def sort(self, Ncol, order):
#     #     """Sort table by given column number.
#     #     """
#     #     try:
#     #         self.layoutAboutToBeChanged.emit()
#     #         self.data = self.data.sort_values(self._data.columns[Ncol], ascending=not order)
#     #         self.layoutChanged.emit()
#     #     except Exception as e:
#     #         print(e)
#
#
# class ReadOnlyDelegate(QStyledItemDelegate):
#     def createEditor(self, parent, option, index):
#         return
#
#
# class EditDelegate(QStyledItemDelegate):
#     def createEditor(self, parent, option, index):
#         return super().createEditor(parent, option, index)


# ------------------------------------------------------------

from PyQt5 import QtCore, QtGui, QtWidgets
from operator import itemgetter


class Materials(QtCore.QAbstractTableModel):
    def __init__(self, materials=[[]], headerdata=None, parent=None, editable=False):
        super(Materials, self).__init__()
        self.materials = materials
        self._editable = editable
        self.check_states = dict()
        self.headerdata = headerdata

    def rowCount(self, parent):
        return len(self.materials)

    def columnCount(self, parent):
        return len(self.materials[0])

    def data(self, index, role):

        if role == QtCore.Qt.DisplayRole:
            row = index.row()
            column = index.column()
            value = self.materials[row][column]
            return value

        if role == QtCore.Qt.EditRole:
            row = index.row()
            column = index.column()
            value = self.materials[row][column]
            return value

        if role == QtCore.Qt.FontRole:
            if index.column() == 0:
                boldfont = QtGui.QFont()
                boldfont.setBold(True)
                return boldfont

        if role == QtCore.Qt.CheckStateRole:
            value = self.check_states.get(QtCore.QPersistentModelIndex(index))
            if value is not None:
                return value

        if role == QtCore.Qt.ToolTipRole:
            row = index.row()
            column = index.column()
            return self.materials[row][column]

    def setData(self, index, value, role=QtCore.Qt.EditRole):
        if role == QtCore.Qt.EditRole:
            row = index.row()
            column = index.column()
            self.materials[row][column] = value
            self.dataChanged.emit(index, index, (role,))
            return True
        if role == QtCore.Qt.CheckStateRole:
            self.check_states[QtCore.QPersistentModelIndex(index)] = value
            self.dataChanged.emit(index, index, (role,))
            return True
        return False

    def headerData(self, section: int, orientation: QtCore.Qt.Orientation, role: int = ...):
        if self.headerdata:
            if orientation == QtCore.Qt.Horizontal and role == QtCore.Qt.DisplayRole:
                # return f"Column {section + 1}"
                return self.headerdata[section]
            if orientation == QtCore.Qt.Vertical and role == QtCore.Qt.DisplayRole:
                return f"{section + 1}"

    def flags(self, index):
        if self._editable:
            return (
                QtCore.Qt.ItemIsEditable
                | QtCore.Qt.ItemIsEnabled
                | QtCore.Qt.ItemIsSelectable
                | QtCore.Qt.ItemIsUserCheckable
            )
        else:
            return (
                QtCore.Qt.ItemIsSelectable
                | QtCore.Qt.ItemIsEnabled
            )

    def sort(self, Ncol, order):
        """Sort table by given column number.
        """
        self.layoutAboutToBeChanged.emit()
        self.materials = sorted(self.materials, key=lambda x: x[Ncol].upper())
        if order == QtCore.Qt.DescendingOrder:
            self.materials.reverse()
        self.layoutChanged.emit()


class ReadOnlyDelegate(QtWidgets.QStyledItemDelegate):
    def createEditor(self, parent, option, index):
        return


class EditDelegate(QtWidgets.QStyledItemDelegate):
    def createEditor(self, parent, option, index):
        return super().createEditor(parent, option, index)