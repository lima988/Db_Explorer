# main_window.py

import sys
import os
import time
import datetime
import psycopg2
import sqlite3 as sqlite
from functools import partial
import pandas as pd
import uuid

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTreeView, QTabWidget,
    QSplitter, QLineEdit, QTextEdit, QComboBox, QTableView, QVBoxLayout, QWidget, QStatusBar, QToolBar, QFileDialog,
    QSizePolicy, QPushButton, QInputDialog, QMessageBox, QMenu, QAbstractItemView, QDialog, QFormLayout, QHBoxLayout,
    QStackedWidget, QLabel, QGroupBox, QDialogButtonBox, QCheckBox, QRadioButton, QStyle, QHeaderView, QFrame
)
from PyQt6.QtGui import (
    QAction, QIcon, QStandardItemModel, QStandardItem, QFont, QMovie, QDesktopServices, QColor, QBrush
)
from PyQt6.QtCore import (
    Qt, QDir, QModelIndex, QSize, QObject, pyqtSignal, QRunnable, QThreadPool, QTimer, QUrl
)

# Importing classes from the dialogs folder
from dialogs.postgres_dialog import PostgresConnectionDialog
from dialogs.sqlite_dialog import SQLiteConnectionDialog
# db Importing modules
import dialogs.db as db
# count rows


class NotificationWidget(QWidget):
    """
    A custom widget for displaying a single notification.
    The position is managed by the NotificationManager.
    """
    #  CHANGE: Add a signal that is emitted when the widget is closed by the user.
    closed = pyqtSignal(QWidget)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | Qt.WindowType.ToolTip
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setObjectName("notificationWidget")

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 5, 10, 5)
        layout.setSpacing(10)

        self.icon_label = QLabel()
        self.message_label = QLabel()
        self.close_button = QPushButton("✕")
        self.close_button.setObjectName("notificationCloseButton")
        self.close_button.setFixedSize(20, 20)
        #  CHANGE: Connect the close button to a method that emits our custom signal.
        self.close_button.clicked.connect(self.close_widget)

        layout.addWidget(self.icon_label)
        layout.addWidget(self.message_label)
        layout.addStretch()
        layout.addWidget(self.close_button)

    def show_message(self, message, is_error=False):
        """
        Sets the content of the notification.
        """
        self.message_label.setText(message)

        if is_error:
            self.setProperty("isError", True)
            icon = self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxCritical)
        else:
            self.setProperty("isError", False)
            # A better icon for success/info
            icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)

        self.icon_label.setPixmap(icon.pixmap(16, 16))

        # Refresh style to apply properties
        self.style().unpolish(self)
        self.style().polish(self)

        self.adjustSize()
        self.show()

    def close_widget(self):
        """
        Emits the 'closed' signal and then closes the widget.
        """
        #  CHANGE: Emit signal before closing, so the manager can catch it.
        self.closed.emit(self)
        self.close()


class NotificationManager:
    """
    Manages the creation, stacking, and display of multiple NotificationWidgets.
    """

    def __init__(self, parent_widget):
        self.parent = parent_widget
        self.notifications = []  # List to keep track of active notifications
        self.spacing = 10
        self.margin = 15

    def show_message(self, message, is_error=False):
        """
        Creates and shows a new notification.
        """
        # Create a new notification widget instance
        notification = NotificationWidget(self.parent)

        # Connect its 'closed' signal to our handler
        notification.closed.connect(self.on_notification_closed)

        # Add the new notification to the beginning of our list
        self.notifications.insert(0, notification)

        # Display the message in the widget
        notification.show_message(message, is_error)

        # Reposition all notifications to stack them correctly
        self.reposition_notifications()

    def on_notification_closed(self, notification_widget):
        """
        Slot to handle a notification being closed.
        """
        try:
            # Remove the closed widget from our list
            self.notifications.remove(notification_widget)
        except ValueError:
            # Widget might have already been removed, so we can ignore it
            pass

        # Reposition the remaining notifications
        self.reposition_notifications()

    def reposition_notifications(self):
        """
        Arranges all active notifications in a stack at the bottom-right
        of the parent widget.
        """
        if not self.parent:
            return

        parent_rect = self.parent.geometry()

        # Calculate the height of the status bar, if it exists
        status_bar_height = 0
        if hasattr(self.parent, 'statusBar') and self.parent.statusBar():
            status_bar_height = self.parent.statusBar().height()

        # Initial Y position for the newest (top) notification
        y = parent_rect.height() - status_bar_height - self.margin

        # Iterate through notifications and place them
        for notification in self.notifications:
            # Adjust Y position based on widget height
            y -= notification.height()

            # Calculate X position
            x = parent_rect.width() - notification.width() - self.margin

            notification.move(x, y)

            # Add spacing for the next notification above it
            y -= self.spacing

# MODIFIED Export Dialog
class ExportDialog(QDialog):
    """
    A custom dialog to get options for exporting data.
    """

    def __init__(self, parent=None, default_filename="export.csv"):
        super().__init__(parent)
        # MODIFIED: Title changed as Import is removed
        self.setWindowTitle("Export Data")
        self.setMinimumWidth(550)

        main_layout = QVBoxLayout(self)
        tab_widget = QTabWidget()
        main_layout.addWidget(tab_widget)

        general_tab = QWidget()
        options_tab = QWidget()
        tab_widget.addTab(general_tab, "General")
        tab_widget.addTab(options_tab, "Options")

        # --- General Tab Layout ---
        general_layout = QFormLayout(general_tab)

        # MODIFIED: Removed Import/Export radio buttons as only Export is supported
        # A simple label to indicate the action
        general_layout.addRow("Action:", QLabel("Export"))

        # Filename
        self.filename_edit = QLineEdit(default_filename)
        browse_btn = QPushButton()
        browse_btn.setIcon(self.style().standardIcon(
            QStyle.StandardPixmap.SP_DirOpenIcon))
        browse_btn.setFixedSize(30, 25)
        browse_btn.clicked.connect(self.browse_file)
        filename_layout = QHBoxLayout()
        filename_layout.addWidget(self.filename_edit)
        filename_layout.addWidget(browse_btn)
        general_layout.addRow("Filename:", filename_layout)

        # Format
        self.format_combo = QComboBox()
        # MODIFIED: Items changed to csv and xlsx
        self.format_combo.addItems(["csv", "xlsx"])
        self.format_combo.setCurrentText("csv")
        # Connect signal to update UI based on format
        self.format_combo.currentTextChanged.connect(self.on_format_change)
        general_layout.addRow("Format:", self.format_combo)

        # Encoding
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(['UTF-8', 'LATIN1', 'windows-1252'])
        self.encoding_combo.setEditable(True)
        general_layout.addRow("Encoding:", self.encoding_combo)

        # --- Options Tab Layout ---
        options_layout = QFormLayout(options_tab)
        self.options_layout = options_layout # to access it later

        self.header_check = QCheckBox("Header")
        self.header_check.setChecked(True)
        options_layout.addRow("Options:", self.header_check)

        # Delimiter and Quote options are specific to CSV
        self.delimiter_label = QLabel("Delimiter:")
        self.delimiter_combo = QComboBox()
        self.delimiter_combo.addItems([',', ';', '|', '\\t'])
        self.delimiter_combo.setEditable(True)
        
        self.quote_label = QLabel("Quote character:")
        self.quote_edit = QLineEdit('"')
        self.quote_edit.setMaxLength(1)

        options_layout.addRow(self.delimiter_label, self.delimiter_combo)
        options_layout.addRow(self.quote_label, self.quote_edit)

        # OK and Cancel buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)
        
        # Set initial state
        self.on_format_change(self.format_combo.currentText())

    def on_format_change(self, format_text):
        """Show/hide options based on the selected format."""
        is_csv = (format_text == 'csv')
        self.encoding_combo.setEnabled(is_csv)
        self.delimiter_label.setVisible(is_csv)
        self.delimiter_combo.setVisible(is_csv)
        self.quote_label.setVisible(is_csv)
        self.quote_edit.setVisible(is_csv)
        
        # Update filename extension
        current_filename = self.filename_edit.text()
        base_name, _ = os.path.splitext(current_filename)
        self.filename_edit.setText(f"{base_name}.{format_text}")


    def browse_file(self):
        # MODIFIED: File filter updated for both CSV and Excel
        file_filter = "CSV Files (*.csv);;Excel Files (*.xlsx);;All Files (*)"
        path, _ = QFileDialog.getSaveFileName(
            self, "Select Output File", self.filename_edit.text(), file_filter)
        if path:
            self.filename_edit.setText(path)

    def get_options(self):
        delimiter = self.delimiter_combo.currentText()
        if delimiter == '\\t':
            delimiter = '\t'

        return {
            "filename": self.filename_edit.text(),
            "encoding": self.encoding_combo.currentText(),
            "format": self.format_combo.currentText(),
            "header": self.header_check.isChecked(),
            "delimiter": delimiter,
            "quote": self.quote_edit.text()
        }


class TablePropertiesDialog(QDialog):
    def __init__(self, item_data, table_name, parent=None):
        super().__init__(parent)
        self.item_data = item_data
        self.table_name = table_name
        self.conn_data = self.item_data['conn_data']
        self.db_type = self.item_data['db_type']
        self.schema_name = self.item_data.get('schema_name')
        self.qualified_table_name = f"{self.schema_name}.{self.table_name}"

        self.setWindowTitle(f"Properties - {self.table_name}")
        self.setMinimumSize(700, 500)

        main_layout = QVBoxLayout(self)
        tab_widget = QTabWidget()
        main_layout.addWidget(tab_widget)

        try:
            general_tab = self._create_general_tab()
            columns_tab = self._create_columns_tab()
            constraints_tab = self._create_constraints_tab()

            tab_widget.addTab(general_tab, "General")
            tab_widget.addTab(columns_tab, "Columns")
            tab_widget.addTab(constraints_tab, "Constraints")
        except Exception as e:
            error_label = QLabel(f"Failed to load table properties:\n{e}")
            error_label.setWordWrap(True)
            main_layout.addWidget(error_label)

        button_box = QHBoxLayout()
        button_box.addStretch()
        ok_button = QPushButton("OK")
        ok_button.clicked.connect(self.accept)
        button_box.addWidget(ok_button)
        main_layout.addLayout(button_box)

    def _create_general_tab(self):
        widget = QWidget()
        layout = QFormLayout(widget)
        layout.setSpacing(10)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapAllRows)

        properties = {}
        if self.db_type == 'postgres':
            properties = self._fetch_postgres_general_properties()
        else:
            properties = self._fetch_sqlite_general_properties()

        self.name_field = QLineEdit(properties.get("Name", ""))
        self.name_field.setReadOnly(True)
        self.owner_combo = QComboBox()
        self.schema_field = QLineEdit(properties.get("Schema", ""))
        self.schema_field.setReadOnly(True)
        self.tablespace_combo = QComboBox()
        self.partitioned_check = QCheckBox()
        self.comment_edit = QTextEdit(properties.get("Comment", ""))

        if self.db_type == 'postgres':
            if "all_owners" in properties:
                self.owner_combo.addItems(properties.get("all_owners", []))
            if "all_tablespaces" in properties:
                self.tablespace_combo.addItems(
                    properties.get("all_tablespaces", []))
            self.owner_combo.setCurrentText(properties.get("Owner", ""))
            self.tablespace_combo.setCurrentText(
                properties.get("Table Space", "default"))
            self.partitioned_check.setChecked(
                properties.get("Is Partitioned", False))
        else:
            self.owner_combo.setEnabled(False)
            self.tablespace_combo.setEnabled(False)
            self.partitioned_check.setEnabled(False)

        layout.addRow("Name:", self.name_field)
        layout.addRow("Owner:", self.owner_combo)
        layout.addRow("Schema:", self.schema_field)
        layout.addRow("Tablespace:", self.tablespace_combo)
        layout.addRow("Partitioned table?:", self.partitioned_check)
        layout.addRow("Comment:", self.comment_edit)

        return widget

    def _create_tag_button(self, text):
        """Creates a styled QPushButton that looks like a tag."""
        button = QPushButton(f"{text}  ×")
        button.setStyleSheet("""
            QPushButton {
                background-color: #e1e1e1;
                border: 1px solid #c1c1c1;
                border-radius: 4px;
                padding: 3px 6px;
                font-size: 9pt;
                text-align: center;
            }
            QPushButton:hover {
                background-color: #d1d1d1;
            }
            QPushButton:pressed {
                background-color: #c1c1c1;
            }
        """)
        button.clicked.connect(lambda: self._remove_inheritance_tag(button))
        return button

    def _remove_inheritance_tag(self, button_to_remove):
        """Removes an inheritance tag from the UI."""
        table_name = button_to_remove.text().split("  ×")[0]
        button_to_remove.deleteLater()

        current_items = [self.add_parent_combo.itemText(
            i) for i in range(self.add_parent_combo.count())]
        if table_name not in current_items:
            self.add_parent_combo.addItem(table_name)
            self.add_parent_combo.model().sort(0)

    def _add_inheritance_tag(self, index):
        """Adds a new inheritance tag to the UI when selected from the combobox."""
        table_to_add = self.add_parent_combo.itemText(index)
        if not table_to_add:
            return

        for i in range(self.inheritance_layout.count()):
            widget = self.inheritance_layout.itemAt(i).widget()
            if isinstance(widget, QPushButton) and widget.text().startswith(table_to_add):
                self.add_parent_combo.setCurrentIndex(0)
                return

        new_tag = self._create_tag_button(table_to_add)
        self.inheritance_layout.insertWidget(
            self.inheritance_layout.indexOf(self.add_parent_combo), new_tag)

        self.add_parent_combo.removeItem(index)
        self.add_parent_combo.setCurrentIndex(0)

    def _create_columns_tab(self):
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 5, 0, 0)

        # "Inherited from table(s)" section
        inheritance_group = QGroupBox("Inherited from table(s)")
        inheritance_frame = QFrame()
        inheritance_frame.setObjectName("inheritanceFrame")
        inheritance_frame.setStyleSheet(
            "#inheritanceFrame { border: 1px solid #a9a9a9; border-radius: 3px; }")

        self.inheritance_layout = QHBoxLayout(inheritance_frame)
        self.inheritance_layout.setContentsMargins(2, 2, 2, 2)
        self.inheritance_layout.setSpacing(4)

        group_layout = QVBoxLayout(inheritance_group)
        group_layout.addWidget(inheritance_frame)
        layout.addWidget(inheritance_group)

        if self.db_type == 'postgres':
            inherited_tables = self._fetch_postgres_inheritance()
            all_tables = self._fetch_all_connection_tables()
            possible_new_parents = sorted(
                [t for t in all_tables if t != self.qualified_table_name and t not in inherited_tables])

            for table_name in inherited_tables:
                tag = self._create_tag_button(table_name)
                self.inheritance_layout.addWidget(tag)

            self.add_parent_combo = QComboBox()
            self.add_parent_combo.addItems([""] + possible_new_parents)
            self.add_parent_combo.setMinimumWidth(150)
            self.add_parent_combo.setStyleSheet("QComboBox { border: none; }")
            self.add_parent_combo.setSizePolicy(
                QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            self.add_parent_combo.activated.connect(self._add_inheritance_tag)

            self.inheritance_layout.addWidget(self.add_parent_combo)
            self.inheritance_layout.addStretch(0)
        else:
            inheritance_group.setEnabled(False)

        # Columns table view
        columns_group = QGroupBox("Columns")
        columns_layout = QVBoxLayout(columns_group)
        layout.addWidget(columns_group)
        table_view = QTableView()
        columns_layout.addWidget(table_view)
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(
            ['', '', 'Name', 'Data type', 'Length/Precision', 'Scale', 'Not NULL?', 'Primary key?', 'Default'])
        table_view.setModel(model)
        table_view.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers)

        columns_data = self._fetch_postgres_columns(
        ) if self.db_type == 'postgres' else self._fetch_sqlite_columns()

        edit_icon = self.style().standardIcon(
            QStyle.StandardPixmap.SP_FileDialogDetailedView)
        delete_icon = self.style().standardIcon(
            QStyle.StandardPixmap.SP_DialogCloseButton)
        gray_brush = QBrush(QColor("gray"))

        for row_idx, row_data in enumerate(columns_data):
            # unpack data: name, type, len, scale, not_null, pk, default, is_local
            is_local = row_data[7] if self.db_type == 'postgres' else True

            # Create items
            edit_item = QStandardItem(edit_icon, "")
            delete_item = QStandardItem(delete_icon, "")
            name_text = f"{row_data[0]} (inherited)" if not is_local else str(
                row_data[0])
            name_item = QStandardItem(name_text)
            type_item = QStandardItem(str(row_data[1]))
            len_item = QStandardItem(str(row_data[2]))
            scale_item = QStandardItem(str(row_data[3]))
            default_item = QStandardItem(str(row_data[6]))
            not_null_item = QStandardItem("")
            pk_item = QStandardItem("")

            all_items = [edit_item, delete_item, name_item, type_item,
                         len_item, scale_item, not_null_item, pk_item, default_item]

            # Style inherited rows
            if not is_local:
                for item in all_items:
                    item.setForeground(gray_brush)
                    # Make inherited rows not selectable
                    flags = item.flags()
                    flags &= ~Qt.ItemFlag.ItemIsSelectable
                    item.setFlags(flags)

            edit_item.setEditable(False)
            delete_item.setEditable(False)
            not_null_item.setEditable(False)
            pk_item.setEditable(False)

            model.appendRow(all_items)

            # Checkboxes
            is_not_null = (row_data[4] == "✔")
            is_pk = (row_data[5] == "✔")
            not_null_switch = QCheckBox()
            not_null_switch.setChecked(is_not_null)
            not_null_switch.setEnabled(False)
            pk_switch = QCheckBox()
            pk_switch.setChecked(is_pk)
            pk_switch.setEnabled(False)

            for col_idx, switch_widget in [(6, not_null_switch), (7, pk_switch)]:
                container = QWidget()
                h_layout = QHBoxLayout(container)
                h_layout.addWidget(switch_widget)
                h_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                h_layout.setContentsMargins(0, 0, 0, 0)
                table_view.setIndexWidget(
                    model.index(row_idx, col_idx), container)

        table_view.resizeColumnsToContents()
        table_view.setColumnWidth(0, 28)
        table_view.setColumnWidth(1, 28)
        return widget

    def _create_constraints_tab(self):
        container_widget = QWidget()
        main_layout = QVBoxLayout(container_widget)
        main_layout.setContentsMargins(0, 5, 0, 0)
        constraints_tab_widget = QTabWidget()
        main_layout.addWidget(constraints_tab_widget)

        constraints_by_type = self._fetch_postgres_constraints(
        ) if self.db_type == 'postgres' else self._fetch_sqlite_constraints()

        tab_definitions = [
            ("Primary Key", 'PRIMARY KEY', ['Name', 'Columns']),
            ("Foreign Key", 'FOREIGN KEY', [
             'Name', 'Columns', 'Referenced Table', 'Referenced Columns']),
            ("Check", 'CHECK', ['Name', 'Definition']),
            ("Unique", 'UNIQUE', ['Name', 'Columns'])
        ]

        for title, key, headers in tab_definitions:
            data = constraints_by_type.get(key, [])
            table_view = QTableView()
            table_view.setEditTriggers(
                QAbstractItemView.EditTrigger.NoEditTriggers)
            table_view.setAlternatingRowColors(True)
            table_view.horizontalHeader().setStretchLastSection(True)

            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(headers)
            table_view.setModel(model)

            for row_data in data:
                items = [QStandardItem(str(item)) for item in row_data]
                model.appendRow(items)

            # Center align all items in the table
            for row in range(model.rowCount()):
                for col in range(model.columnCount()):
                    item = model.item(row, col)
                    if item:
                        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)

            table_view.resizeColumnsToContents()
            constraints_tab_widget.addTab(table_view, title)

        return container_widget

    def _fetch_sqlite_general_properties(self):
        return {
            "Name": self.table_name, "Owner": "N/A", "Schema": "main",
            "Table Space": "N/A", "Comment": "N/A"
        }

    def _fetch_all_connection_tables(self):
        """Fetches all tables (schema.table) from the current connection for the inheritance combobox."""
        tables = []
        if self.db_type != 'postgres':
            return tables

        conn = None
        try:
            expected_keys = ['host', 'port', 'database', 'user', 'password']
            pg_conn_data = {key: self.conn_data.get(
                key) for key in expected_keys}
            conn = db.create_postgres_connection(**pg_conn_data)
            cursor = conn.cursor()

            query = """
                SELECT table_schema || '.' || table_name AS qualified_name
                FROM information_schema.tables
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                AND table_type = 'BASE TABLE'
                ORDER BY table_schema, table_name;
            """
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]

        except Exception as e:
            QMessageBox.critical(
                self, "DB Error", f"Error fetching connection tables:\n{e}")
        finally:
            if conn:
                conn.close()
        return tables

    def _fetch_postgres_inheritance(self):
        inherited_from = []
        conn = None
        try:
            expected_keys = ['host', 'port', 'database', 'user', 'password']
            pg_conn_data = {key: self.conn_data.get(
                key) for key in expected_keys}
            conn = db.create_postgres_connection(**pg_conn_data)
            cursor = conn.cursor()
            query = """
                SELECT pn.nspname || '.' || parent.relname AS parent_table
                FROM pg_inherits
                JOIN pg_class AS child ON pg_inherits.inhrelid = child.oid
                JOIN pg_namespace AS cns ON child.relnamespace = cns.oid
                JOIN pg_class AS parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_namespace AS pn ON parent.relnamespace = pn.oid
                WHERE child.relname = %s AND cns.nspname = %s;
            """
            cursor.execute(query, (self.table_name, self.schema_name))
            inherited_from = [row[0] for row in cursor.fetchall()]
        finally:
            if conn:
                conn.close()
        return inherited_from

    def _fetch_postgres_general_properties(self):
        props = {"Name": self.table_name, "Schema": self.schema_name}
        conn = None
        try:
            expected_keys = ['host', 'port', 'database', 'user', 'password']
            pg_conn_data = {key: self.conn_data.get(
                key) for key in expected_keys}
            conn = db.create_postgres_connection(**pg_conn_data)
            cursor = conn.cursor()
            query = """
                SELECT u.usename as owner, ts.spcname as tablespace, d.description, c.relispartition
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_user u ON u.usesysid = c.relowner
                LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace
                LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
                WHERE n.nspname = %s AND c.relname = %s
            """
            cursor.execute(query, (self.schema_name, self.table_name))
            res = cursor.fetchone()
            if res:
                props["Owner"] = res[0] or "N/A"
                props["Table Space"] = res[1] or "default"
                props["Comment"] = res[2] or ""
                props["Is Partitioned"] = res[3]

            cursor.execute(
                "SELECT rolname FROM pg_roles WHERE rolcanlogin = true ORDER BY rolname;")
            props["all_owners"] = [row[0] for row in cursor.fetchall()]
            cursor.execute(
                "SELECT spcname FROM pg_tablespace ORDER BY spcname;")
            props["all_tablespaces"] = ["default"] + [row[0]
                                                      for row in cursor.fetchall()]
        finally:
            if conn:
                conn.close()
        return props

    def _fetch_sqlite_columns(self):
        columns = []
        conn = None
        try:
            conn = db.create_sqlite_connection(self.conn_data['db_path'])
            cursor = conn.cursor()
            cursor.execute(f'PRAGMA table_info("{self.table_name}");')
            pk_cols = {row[1] for row in cursor.fetchall() if row[5] > 0}
            cursor.execute(f'PRAGMA table_info("{self.table_name}");')
            for row in cursor.fetchall():
                columns.append([
                    row[1], row[2], "", "", "✔" if row[3] else "",
                    "✔" if row[1] in pk_cols else "", row[4] or ""
                ])
        finally:
            if conn:
                conn.close()
        return columns

    def _fetch_postgres_columns(self):
        columns = []
        conn = None
        try:
            expected_keys = ['host', 'port', 'database', 'user', 'password']
            pg_conn_data = {key: self.conn_data.get(
                key) for key in expected_keys}
            conn = db.create_postgres_connection(**pg_conn_data)
            cursor = conn.cursor()

            pk_query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s AND tc.table_name = %s;
            """
            cursor.execute(pk_query, (self.schema_name, self.table_name))
            pk_columns = {row[0] for row in cursor.fetchall()}

            col_query = """
                SELECT
                    c.column_name,
                    c.udt_name,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    c.is_nullable,
                    c.column_default,
                    a.attislocal
                FROM information_schema.columns AS c
                JOIN pg_catalog.pg_class AS pc ON c.table_name = pc.relname
                JOIN pg_catalog.pg_namespace AS pn ON pc.relnamespace = pn.oid AND c.table_schema = pn.nspname
                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = pc.oid AND a.attname = c.column_name
                WHERE c.table_schema = %s AND c.table_name = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY c.ordinal_position;
            """
            cursor.execute(col_query, (self.schema_name, self.table_name))
            for row in cursor.fetchall():
                length_precision = row[2] if row[2] is not None else row[3]
                columns.append([
                    row[0], row[1], length_precision or "", row[4] or "",
                    "✔" if row[5] == "NO" else "", "✔" if row[0] in pk_columns else "", row[6] or "",
                    row[7]  # attislocal
                ])
        finally:
            if conn:
                conn.close()
        return columns

    def _fetch_sqlite_constraints(self):
        constraints = {'PRIMARY KEY': [],
                       'FOREIGN KEY': [], 'UNIQUE': [], 'CHECK': []}
        conn = None
        try:
            conn = db.create_sqlite_connection(self.conn_data['db_path'])
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{self.table_name}';")
            sql_def_row = cursor.fetchone()
            sql_def = sql_def_row[0] if sql_def_row else ""

            cursor.execute(f'PRAGMA table_info("{self.table_name}");')
            pk_info = [row for row in cursor.fetchall() if row[5] > 0]
            if pk_info:
                pk_name = f"PK_{self.table_name}"
                if "CONSTRAINT" in sql_def.upper() and "PRIMARY KEY" in sql_def.upper():
                    for line in sql_def.split('\n'):
                        if "CONSTRAINT" in line.upper() and "PRIMARY KEY" in line.upper():
                            pk_name = line.split()[1].strip('`"')
                            break
                pk_cols = [row[1] for row in pk_info]
                constraints['PRIMARY KEY'].append(
                    [pk_name, ", ".join(pk_cols)])

            cursor.execute(f'PRAGMA foreign_key_list("{self.table_name}");')
            fks = {}
            for row in cursor.fetchall():
                fk_id, _, ref_table, from_col, to_col, _, _, _ = row
                if fk_id not in fks:
                    fks[fk_id] = {'from': [], 'to': [], 'ref_table': ref_table}
                fks[fk_id]['from'].append(from_col)
                fks[fk_id]['to'].append(to_col)

            fk_names = {}
            if sql_def:
                fk_counter = 0
                for line in sql_def.split('\n'):
                    if line.strip().upper().startswith("CONSTRAINT") and "FOREIGN KEY" in line.upper():
                        fk_names[fk_counter] = line.split()[1].strip('`"')
                        fk_counter += 1
            for i, fk_id in enumerate(fks):
                name = fk_names.get(
                    i, f"FK_{self.table_name}_{fks[fk_id]['ref_table']}_{fk_id}")
                constraints['FOREIGN KEY'].append([name, ", ".join(
                    fks[fk_id]['from']), fks[fk_id]['ref_table'], ", ".join(fks[fk_id]['to'])])

            cursor.execute(f'PRAGMA index_list("{self.table_name}")')
            for index in cursor.fetchall():
                if index[2] == 1 and "sqlite_autoindex" not in index[1]:
                    cursor.execute(f'PRAGMA index_info("{index[1]}")')
                    cols = ", ".join([info[2] for info in cursor.fetchall()])
                    constraints['UNIQUE'].append([index[1], cols])

            if sql_def:
                for line in sql_def.split('\n'):
                    line = line.strip().rstrip(',')
                    upper_line = line.upper()
                    if upper_line.startswith("CONSTRAINT") and "CHECK" in upper_line:
                        constraints['CHECK'].append(
                            [line.split()[1].strip('`"'), line[line.find('('):].strip()])
                    elif upper_line.startswith("CHECK"):
                        constraints['CHECK'].append(
                            [f"CK_{self.table_name}", line[line.find('('):].strip()])
        finally:
            if conn:
                conn.close()
        return constraints

    def _fetch_postgres_constraints(self):
        constraints = {'PRIMARY KEY': [],
                       'FOREIGN KEY': [], 'UNIQUE': [], 'CHECK': []}
        conn = None
        try:
            expected_keys = ['host', 'port', 'database', 'user', 'password']
            pg_conn_data = {key: self.conn_data.get(
                key) for key in expected_keys}
            conn = db.create_postgres_connection(**pg_conn_data)
            cursor = conn.cursor()

            query_key = """
                SELECT tc.constraint_name, tc.constraint_type, STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS columns
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = %s AND tc.table_schema = %s AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                GROUP BY tc.constraint_name, tc.constraint_type;
            """
            cursor.execute(query_key, (self.table_name, self.schema_name))
            for name, type, cols in cursor.fetchall():
                constraints[type].append([name, cols])

            query_fk = """
                SELECT rc.constraint_name,
                       STRING_AGG(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS foreign_columns,
                       ccu.table_schema AS primary_table_schema, ccu.table_name AS primary_table_name,
                       STRING_AGG(ccu.column_name, ', ' ORDER BY ccu.ordinal_position) AS primary_columns
                FROM information_schema.referential_constraints AS rc
                JOIN information_schema.key_column_usage AS kcu ON kcu.constraint_name = rc.constraint_name AND kcu.table_schema = %s
                JOIN information_schema.key_column_usage AS ccu ON ccu.constraint_name = rc.unique_constraint_name AND ccu.table_schema = rc.unique_constraint_schema
                WHERE kcu.table_name = %s AND kcu.table_schema = %s
                GROUP BY rc.constraint_name, ccu.table_schema, ccu.table_name;
            """
            cursor.execute(query_fk, (self.schema_name,
                           self.table_name, self.schema_name))
            for name, f_cols, p_schema, p_table, p_cols in cursor.fetchall():
                constraints['FOREIGN KEY'].append(
                    [name, f_cols, f"{p_schema}.{p_table}", p_cols])

            query_check = """
                SELECT con.conname, pg_get_constraintdef(con.oid) as definition
                FROM pg_constraint con
                JOIN pg_class c ON c.oid = con.conrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = %s AND con.contype = 'c';
            """
            cursor.execute(query_check, (self.table_name, self.schema_name))
            for name, definition in cursor.fetchall():
                constraints['CHECK'].append([name, definition])
        finally:
            if conn:
                conn.close()
        return constraints


# Signals class for QRunnable worker


class QuerySignals(QObject):
    finished = pyqtSignal(dict, str, list, list, int, float, bool)
    error = pyqtSignal(str)


# Signals for the Background Process Worker
class ProcessSignals(QObject):
    started = pyqtSignal(str, dict)  # process_id, initial_data
    finished = pyqtSignal(str, str, float)  # process_id, message, time_taken
    error = pyqtSignal(str, str)  # process_id, error_message


# MODIFIED Worker for Asynchronous Export (handles both CSV and Excel)
class RunnableExport(QRunnable):
    def __init__(self, process_id, item_data, table_name, export_options, signals):
        super().__init__()
        self.process_id = process_id
        self.item_data = item_data
        self.table_name = table_name
        self.export_options = export_options
        self.signals = signals

    def run(self):
        start_time = time.time()
        conn = None
        try:
            conn_data = self.item_data['conn_data']
            db_type = self.item_data.get('db_type')

            if db_type == 'sqlite':
                conn = db.create_sqlite_connection(conn_data["db_path"])
                query = f'SELECT * FROM "{self.table_name}"'
            elif db_type == 'postgres':
                conn = db.create_postgres_connection(
                    host=conn_data["host"], database=conn_data["database"],
                    user=conn_data["user"], password=conn_data["password"],
                    port=int(conn_data["port"])
                )
                schema_name = self.item_data.get("schema_name")
                query = f'SELECT * FROM "{schema_name}"."{self.table_name}"'
            else:
                raise ValueError("Unsupported database type for export.")

            if not conn:
                raise ConnectionError(
                    "Failed to connect to the database for export.")

            df = pd.read_sql_query(query, conn)
            file_path = self.export_options['filename']
            file_format = self.export_options['format']

            # MODIFIED: Logic to handle different file formats
            if file_format == 'xlsx':
                # Use pandas to_excel method to save the DataFrame
                # Note: openpyxl must be installed (pip install openpyxl)
                df.to_excel(
                    file_path,
                    index=False,
                    header=self.export_options['header']
                )
            else: # Default to CSV
                df.to_csv(
                    file_path,
                    index=False,
                    header=self.export_options['header'],
                    sep=self.export_options['delimiter'],
                    encoding=self.export_options['encoding'],
                    quotechar=self.export_options['quote']
                )

            time_taken = time.time() - start_time
            success_message = f"Successfully exported {len(df)} rows to {os.path.basename(file_path)}"
            self.signals.finished.emit(
                self.process_id, success_message, time_taken)

        except Exception as e:
            self.signals.error.emit(
                self.process_id, f"An error occurred during export: {e}")
        finally:
            if conn:
                conn.close()


class RunnableQuery(QRunnable):
    def __init__(self, conn_data, query, signals):
        super().__init__()
        self.conn_data = conn_data
        self.query = query
        self.signals = signals
        self._is_cancelled = False

    def cancel(self):
        self._is_cancelled = True

    def run(self):
        conn = None
        try:
            start_time = time.time()
            if not self.conn_data:
                raise ConnectionError("Incomplete connection information.")

            if "db_path" in self.conn_data and self.conn_data["db_path"]:
                conn = db.create_sqlite_connection(self.conn_data["db_path"])
            else:
                conn = db.create_postgres_connection(
                    host=self.conn_data["host"], database=self.conn_data["database"],
                    user=self.conn_data["user"], password=self.conn_data["password"],
                    port=int(self.conn_data["port"])
                )

            if not conn:
                raise ConnectionError(
                    "Failed to establish database connection.")

            cursor = conn.cursor()
            cursor.execute(self.query)

            if self._is_cancelled:
                conn.close()
                return

            row_count = 0
            is_select_query = self.query.lower().strip().startswith("select")
            results = []
            columns = []

            if is_select_query:
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    if not self._is_cancelled:
                        results = cursor.fetchall()
                        row_count = len(results)
                else:
                    row_count = 0
            else:
                conn.commit()
                row_count = cursor.rowcount if cursor.rowcount != -1 else 0

            if self._is_cancelled:
                conn.close()
                return

            elapsed_time = time.time() - start_time
            self.signals.finished.emit(
                self.conn_data, self.query, results, columns, row_count, elapsed_time, is_select_query)

        except Exception as e:
            if not self._is_cancelled:
                self.signals.error.emit(str(e))
        finally:
            if conn:
                conn.close()


class MainWindow(QMainWindow):
    # QUERY_TIMEOUT = 60000

    def __init__(self):
        super().__init__()
        self.setWindowTitle("SQL Client")
        self.setGeometry(100, 100, 1200, 800)

        self.thread_pool = QThreadPool.globalInstance()
        self.tab_timers = {}
        self.running_queries = {}

        self._create_actions()
        self._create_menu()
        self._create_centered_toolbar()

        self.main_splitter = QSplitter(Qt.Orientation.Horizontal)
        self.setCentralWidget(self.main_splitter)

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status_message_label = QLabel("Ready")
        self.status.addWidget(self.status_message_label)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        self.tree = QTreeView()
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.show_context_menu)
        self.tree.clicked.connect(self.item_clicked)
        self.tree.setSelectionMode(
            QAbstractItemView.SelectionMode.SingleSelection)
        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(['Object Explorer'])
        self.tree.setModel(self.model)

        self.left_vertical_splitter = QSplitter(Qt.Orientation.Vertical)
        self.left_vertical_splitter.addWidget(self.tree)

        self.schema_tree = QTreeView()
        self.schema_model = QStandardItemModel()
        self.schema_model.setHorizontalHeaderLabels(["Database Schema"])
        self.schema_tree.setModel(self.schema_model)
        self.schema_tree.setContextMenuPolicy(
            Qt.ContextMenuPolicy.CustomContextMenu)
        self.schema_tree.customContextMenuRequested.connect(
            self.show_schema_context_menu)
        self.left_vertical_splitter.addWidget(self.schema_tree)

        self.left_vertical_splitter.setSizes([240, 360])
        left_layout.addWidget(self.left_vertical_splitter)
        self.main_splitter.addWidget(left_panel)

        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        add_tab_btn = QPushButton("New")
        add_tab_btn.clicked.connect(self.add_tab)
        self.tab_widget.setCornerWidget(add_tab_btn)
        self.main_splitter.addWidget(self.tab_widget)

        # <<< MODIFIED >>> Processes tab will be created on demand, not at startup.
        self.processes_tab = None

        self.thread_monitor_timer = QTimer()
        self.thread_monitor_timer.timeout.connect(
            self.update_thread_pool_status)
        self.thread_monitor_timer.start(1000)

        self.load_data()
        self.add_tab()  # Add initial worksheet
        self.main_splitter.setSizes([280, 920])
        # count_rows
        # self.notification_widget = NotificationWidget(self)
        # self.notification_widget.hide()
        self.notification_manager = NotificationManager(self)
        self._apply_styles()

    # <<< MODIFIED >>> This method now checks if the tab already exists.
    def _create_processes_tab(self):
        # If tab already exists, do nothing.
        if self.processes_tab is not None:
            return

        self.processes_tab = QWidget()
        self.processes_tab.setObjectName("ProcessesTab")
        layout = QVBoxLayout(self.processes_tab)
        layout.setContentsMargins(5, 5, 5, 5)

        self.processes_view = QTableView()
        self.processes_view.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers)
        self.processes_view.setSelectionBehavior(
            QAbstractItemView.SelectionBehavior.SelectRows)
        self.processes_view.setAlternatingRowColors(True)
        self.processes_view.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.processes_view)

        self.processes_model = QStandardItemModel()
        self.processes_model.setHorizontalHeaderLabels([
            "PID", "Type", "Status", "Server", "Object", "Time Taken (sec)", "Start Time", "Details"
        ])
        self.processes_view.setModel(self.processes_model)

        self.processes_view.setColumnWidth(0, 150)
        self.processes_view.setColumnWidth(1, 100)
        self.processes_view.setColumnWidth(2, 100)
        self.processes_view.setColumnWidth(3, 150)
        self.processes_view.setColumnWidth(4, 150)
        self.processes_view.setColumnWidth(5, 120)
        self.processes_view.setColumnWidth(6, 150)

        self.tab_widget.addTab(self.processes_tab, QIcon(
            "assets/process_icon.png"), "Processes")

    def _create_actions(self):
        self.exit_action = QAction(QIcon("assets/exit_icon.png"), "Exit", self)
        self.exit_action.triggered.connect(self.close)
        self.execute_action = QAction(
            QIcon("assets/execute_icon.png"), "Execute", self)
        self.execute_action.triggered.connect(self.execute_query)
        self.cancel_action = QAction(
            QIcon("assets/cancel_icon.png"), "Cancel", self)
        self.cancel_action.triggered.connect(self.cancel_current_query)
        self.cancel_action.setEnabled(False)
        self.undo_action = QAction("Undo", self)
        self.undo_action.triggered.connect(self.undo_text)
        self.redo_action = QAction("Redo", self)
        self.redo_action.triggered.connect(self.redo_text)
        self.cut_action = QAction("Cut", self)
        self.cut_action.triggered.connect(self.cut_text)
        self.copy_action = QAction("Copy", self)
        self.copy_action.triggered.connect(self.copy_text)
        self.paste_action = QAction("Paste", self)
        self.paste_action.triggered.connect(self.paste_text)
        self.delete_action = QAction("Delete", self)
        self.delete_action.triggered.connect(self.delete_text)
        self.query_tool_action = QAction("Query Tool", self)
        self.query_tool_action.triggered.connect(self.add_tab)
        self.restore_action = QAction("Restore Layout", self)
        self.restore_action.triggered.connect(self.restore_tool)
        self.refresh_action = QAction("Refresh Explorer", self)
        self.refresh_action.triggered.connect(self.refresh_object_explorer)
        self.minimize_action = QAction("Minimize", self)
        self.minimize_action.triggered.connect(self.showMinimized)
        self.zoom_action = QAction("Zoom", self)
        self.zoom_action.triggered.connect(self.toggle_maximize)
        self.sqlite_help_action = QAction("SQLite Website", self)
        self.sqlite_help_action.triggered.connect(
            lambda: self.open_help_url("https://www.sqlite.org/"))
        self.postgres_help_action = QAction("PostgreSQL Website", self)
        self.postgres_help_action.triggered.connect(
            lambda: self.open_help_url("https://www.postgresql.org/"))
        self.oracle_help_action = QAction("Oracle Website", self)
        self.oracle_help_action.triggered.connect(
            lambda: self.open_help_url("https://www.oracle.com/database/"))

        # New "About" action
        self.about_action = QAction("About", self)
        self.about_action.triggered.connect(self.show_about_dialog)

    def _create_menu(self):
        menubar = self.menuBar()
        file_menu = menubar.addMenu("&File")
        file_menu.addAction(self.exit_action)
        edit_menu = menubar.addMenu("&Edit")
        edit_menu.addAction(self.undo_action)
        edit_menu.addAction(self.redo_action)
        edit_menu.addSeparator()
        edit_menu.addAction(self.cut_action)
        edit_menu.addAction(self.copy_action)
        edit_menu.addAction(self.paste_action)
        edit_menu.addAction(self.delete_action)
        actions_menu = menubar.addMenu("&Actions")
        actions_menu.addAction(self.execute_action)
        actions_menu.addAction(self.cancel_action)
        tools_menu = menubar.addMenu("&Tools")
        tools_menu.addAction(self.query_tool_action)
        tools_menu.addAction(self.refresh_action)
        tools_menu.addAction(self.restore_action)
        window_menu = menubar.addMenu("&Window")
        window_menu.addAction(self.minimize_action)
        window_menu.addAction(self.zoom_action)
        window_menu.addSeparator()
        close_action = QAction("Close", self)
        close_action.triggered.connect(self.close)
        window_menu.addAction(close_action)
        help_menu = menubar.addMenu("&Help")
        help_menu.addAction(self.sqlite_help_action)
        help_menu.addAction(self.postgres_help_action)
        help_menu.addAction(self.oracle_help_action)
        help_menu.addSeparator()
        help_menu.addAction(self.about_action)

    def _create_centered_toolbar(self):
        toolbar = QToolBar("Main Toolbar")
        toolbar.setToolButtonStyle(Qt.ToolButtonStyle.ToolButtonTextBesideIcon)
        left_spacer = QWidget()
        left_spacer.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        right_spacer = QWidget()
        right_spacer.setSizePolicy(
            QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        toolbar.addWidget(left_spacer)
        toolbar.addAction(self.exit_action)
        toolbar.addAction(self.execute_action)
        toolbar.addAction(self.cancel_action)
        toolbar.addWidget(right_spacer)
        self.addToolBar(toolbar)

     # --- New Handler Method for "About" Dialog ---
    def show_about_dialog(self):
        """
        Displays the About dialog for the application.
        """
        about_title = "About SQL Client"
        about_text = """
        <b>SQL Client Application</b>
        <p>Version 1.0.0</p>
        <p>This is a versatile SQL client designed to connect to and manage
        multiple database systems including PostgreSQL and SQLite.</p>
        <p><b>Features:</b></p>
        <ul>
            <li>Object Explorer for database schemas</li>
            <li>Multi-tab query editor with syntax highlighting</li>
            <li>Query history per connection</li>
            <li>Asynchronous query execution to keep the UI responsive</li>
        </ul>
        <p>Developed to provide a simple and effective tool for database management.</p>
        """
        QMessageBox.about(self, about_title, about_text)

    def _get_current_editor(self):
        current_tab = self.tab_widget.currentWidget()
        if not current_tab or (self.processes_tab and current_tab == self.processes_tab):
            return None
        editor_stack = current_tab.findChild(QStackedWidget, "editor_stack")
        if editor_stack and editor_stack.currentIndex() == 0:
            return current_tab.findChild(QTextEdit, "query_editor")
        return None

    def undo_text(self):
        editor = self._get_current_editor()
        if editor:
            editor.undo()

    def redo_text(self):
        editor = self._get_current_editor()
        if editor:
            editor.redo()

    def cut_text(self):
        editor = self._get_current_editor()
        if editor:
            editor.cut()

    def copy_text(self):
        editor = self._get_current_editor()
        if editor:
            editor.copy()

    def paste_text(self):
        editor = self._get_current_editor()
        if editor:
            editor.paste()

    def delete_text(self):
        editor = self._get_current_editor()
        if editor:
            editor.textCursor().removeSelectedText()

    def restore_tool(self):
        self.main_splitter.setSizes([280, 920])
        self.left_vertical_splitter.setSizes([240, 360])
        current_tab = self.tab_widget.currentWidget()
        if current_tab and (not self.processes_tab or current_tab != self.processes_tab):
            tab_splitter = current_tab.findChild(
                QSplitter, "tab_vertical_splitter")
            if tab_splitter:
                tab_splitter.setSizes([300, 300])
        self.status.showMessage("Layout restored to defaults.", 3000)

    def refresh_object_explorer(self):
        self.load_data()
        self.status.showMessage("Object Explorer refreshed.", 3000)

    def toggle_maximize(self):
        if self.isMaximized():
            self.showNormal()
        else:
            self.showMaximized()

    def open_help_url(self, url_string):
        url = QUrl(url_string)
        if not QDesktopServices.openUrl(url):
            QMessageBox.warning(
                self, "Open URL", f"Could not open URL: {url_string}")

    def update_thread_pool_status(self):
        active = self.thread_pool.activeThreadCount()
        max_threads = self.thread_pool.maxThreadCount()
        self.status.showMessage(
            f"ThreadPool: {active} active of {max_threads}", 3000)

    def _apply_styles(self):
        # Using the user-provided hex codes
        primary_color = "#D3D3D3"  # Light Gray
        header_color = "#A9A9A9"   # Dark Gray
        selection_color = "#A9A9A9"  # Dark Gray for selection
        text_color_on_primary = "#000000"  # Black text on light gray
        alternate_row_color = "#f0f0f0"
        border_color = "#A9A9A9"  # Dark Gray for borders

        style_sheet = f"""
            QMainWindow, QToolBar, QStatusBar {{
                background-color: {primary_color};
                color: {text_color_on_primary};
            }}
            QTreeView {{
                background-color: white;
                alternate-background-color: {alternate_row_color};
                border: 1px solid {border_color};
            }}
            QTableView {{
                alternate-background-color: {alternate_row_color};
                background-color: white;
                gridline-color: #d0d0d0;
                border: 1px solid {border_color};
                font-family: Arial, sans-serif;
                font-size: 9pt;
            }}
            QTableView::item {{ 
                padding: 4px; 
            }}
            QTableView::item:selected {{ 
                background-color: {selection_color}; 
                color: white; 
            }}
            QHeaderView::section {{
                background-color: {header_color};
                color: white;
                padding: 6px;
                border: 1px solid {border_color};
                font-weight: bold;
                font-size: 9pt;
            }}
            QTableView QTableCornerButton::section {{
                background-color: {header_color};
                border: 1px solid {border_color};
            }}
            #resultsHeader QPushButton, #editorHeader QPushButton {{
                background-color: #ffffff;
                border: 1px solid {border_color};
                padding: 5px 15px;
                font-size: 9pt;
            }}
            #resultsHeader QPushButton:hover, #editorHeader QPushButton:hover {{
                background-color: {primary_color};
            }}
            #resultsHeader QPushButton:checked, #editorHeader QPushButton:checked {{
                background-color: {selection_color};
                border-bottom: 1px solid {selection_color};
                font-weight: bold;
                color: white;
            }}
            #resultsHeader, #editorHeader {{
                background-color: {alternate_row_color};
                padding-bottom: -1px;
            }}
            #messageView, #history_details_view, QTextEdit {{
                font-family: Consolas, monospace;
                font-size: 10pt;
                background-color: white;
                border: 1px solid {border_color};
            }}
            #tab_status_label {{
                padding: 3px 5px;
                background-color: {alternate_row_color};
                border-top: 1px solid {border_color};
            }}
            QGroupBox {{
                font-size: 9pt;
                font-weight: bold;
                color: {text_color_on_primary};
            }}
            QTabWidget::pane {{
                border-top: 1px solid {border_color};
            }}
            QTabBar::tab {{
                background: #E0E0E0;
                border: 1px solid {border_color};
                padding: 5px 10px;
                border-bottom: none;
            }}
            QTabBar::tab:selected {{
                background: {selection_color};
                color: white;
            }}
            QComboBox {{
                border: 1px solid {border_color};
                padding: 2px;
                background-color: white;
            }}
        """
        self.setStyleSheet(style_sheet)

    def add_tab(self):
        tab_content = QWidget()
        layout = QVBoxLayout(tab_content)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        db_combo_box = QComboBox()
        db_combo_box.setObjectName("db_combo_box")
        layout.addWidget(db_combo_box)
        self.load_joined_items(db_combo_box)
        main_vertical_splitter = QSplitter(Qt.Orientation.Vertical)
        main_vertical_splitter.setObjectName("tab_vertical_splitter")
        layout.addWidget(main_vertical_splitter)
        editor_container = QWidget()
        editor_layout = QVBoxLayout(editor_container)
        editor_layout.setContentsMargins(0, 0, 0, 0)
        editor_layout.setSpacing(0)
        editor_header = QWidget()
        editor_header.setObjectName("editorHeader")
        editor_header_layout = QHBoxLayout(editor_header)
        editor_header_layout.setContentsMargins(5, 2, 5, 0)
        editor_header_layout.setSpacing(2)
        query_view_btn = QPushButton("Query")
        history_view_btn = QPushButton("Query History")
        query_view_btn.setCheckable(True)
        history_view_btn.setCheckable(True)
        query_view_btn.setChecked(True)
        editor_header_layout.addWidget(query_view_btn)
        editor_header_layout.addWidget(history_view_btn)
        editor_header_layout.addStretch()
        editor_layout.addWidget(editor_header)
        editor_stack = QStackedWidget()
        editor_stack.setObjectName("editor_stack")
        text_edit = QTextEdit()
        text_edit.setPlaceholderText("Write Query")
        text_edit.setObjectName("query_editor")
        editor_stack.addWidget(text_edit)
        history_widget = QSplitter(Qt.Orientation.Horizontal)
        history_list_view = QTreeView()
        history_list_view.setObjectName("history_list_view")
        history_list_view.setHeaderHidden(True)
        history_list_view.setEditTriggers(
            QAbstractItemView.EditTrigger.NoEditTriggers)
        history_details_group = QGroupBox("Query Details")
        history_details_layout = QVBoxLayout(history_details_group)
        history_details_view = QTextEdit()
        history_details_view.setObjectName("history_details_view")
        history_details_view.setReadOnly(True)
        history_details_layout.addWidget(history_details_view)
        history_button_layout = QHBoxLayout()
        copy_history_btn = QPushButton("Copy")
        copy_to_edit_btn = QPushButton("Copy to Edit Query")
        remove_history_btn = QPushButton("Remove")
        remove_all_history_btn = QPushButton("Remove All")
        history_button_layout.addStretch()
        history_button_layout.addWidget(copy_history_btn)
        history_button_layout.addWidget(copy_to_edit_btn)
        history_button_layout.addWidget(remove_history_btn)
        history_button_layout.addWidget(remove_all_history_btn)
        history_details_layout.addLayout(history_button_layout)
        history_widget.addWidget(history_list_view)
        history_widget.addWidget(history_details_group)
        history_widget.setSizes([400, 400])
        editor_stack.addWidget(history_widget)
        editor_layout.addWidget(editor_stack)
        main_vertical_splitter.addWidget(editor_container)

        def switch_editor_view(index):
            editor_stack.setCurrentIndex(index)
            query_view_btn.setChecked(index == 0)
            history_view_btn.setChecked(index == 1)
            if index == 1:
                self.load_connection_history(tab_content)
        query_view_btn.clicked.connect(lambda: switch_editor_view(0))
        history_view_btn.clicked.connect(lambda: switch_editor_view(1))
        db_combo_box.currentIndexChanged.connect(lambda: editor_stack.currentIndex(
        ) == 1 and self.load_connection_history(tab_content))
        history_list_view.clicked.connect(
            lambda index: self.display_history_details(index, tab_content))
        copy_history_btn.clicked.connect(
            lambda: self.copy_history_query(tab_content))
        copy_to_edit_btn.clicked.connect(
            lambda: self.copy_history_to_editor(tab_content))
        remove_history_btn.clicked.connect(
            lambda: self.remove_selected_history(tab_content))
        remove_all_history_btn.clicked.connect(
            lambda: self.remove_all_history_for_connection(tab_content))
        results_container = QWidget()
        results_layout = QVBoxLayout(results_container)
        results_layout.setContentsMargins(0, 5, 0, 0)
        results_layout.setSpacing(0)
        results_header = QWidget()
        results_header.setObjectName("resultsHeader")
        header_layout = QHBoxLayout(results_header)
        header_layout.setContentsMargins(5, 2, 5, 0)
        header_layout.setSpacing(2)
        output_btn = QPushButton("Output")
        message_btn = QPushButton("Message")
        notification_btn = QPushButton("Notification")
        output_btn.setCheckable(True)
        message_btn.setCheckable(True)
        notification_btn.setCheckable(True)
        output_btn.setChecked(True)
        header_layout.addWidget(output_btn)
        header_layout.addWidget(message_btn)
        header_layout.addWidget(notification_btn)
        header_layout.addStretch()
        results_layout.addWidget(results_header)
        results_stack = QStackedWidget()
        results_stack.setObjectName("results_stacked_widget")
        table_view = QTableView()
        table_view.setObjectName("result_table")
        table_view.setAlternatingRowColors(True)
        results_stack.addWidget(table_view)
        message_view = QTextEdit()
        message_view.setObjectName("message_view")
        message_view.setReadOnly(True)
        results_stack.addWidget(message_view)
        notification_view = QLabel("Notifications will appear here.")
        notification_view.setAlignment(Qt.AlignmentFlag.AlignCenter)
        results_stack.addWidget(notification_view)
        spinner_overlay_widget = QWidget()
        spinner_layout = QHBoxLayout(spinner_overlay_widget)
        spinner_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        spinner_movie = QMovie("assets/spinner.gif")
        spinner_label = QLabel()
        spinner_label.setObjectName("spinner_label")
        if not spinner_movie.isValid():
            spinner_label.setText("Loading...")
        else:
            spinner_label.setMovie(spinner_movie)
            spinner_movie.setScaledSize(QSize(32, 32))
        loading_text_label = QLabel("Waiting for query to complete")
        font = QFont()
        font.setPointSize(10)
        loading_text_label.setFont(font)
        loading_text_label.setStyleSheet("color: #555;")
        spinner_layout.addWidget(spinner_label)
        spinner_layout.addWidget(loading_text_label)
        results_stack.addWidget(spinner_overlay_widget)
        results_layout.addWidget(results_stack)
        tab_status_label = QLabel("Ready")
        tab_status_label.setObjectName("tab_status_label")
        results_layout.addWidget(tab_status_label)
        button_group = [output_btn, message_btn, notification_btn]

        def switch_results_view(index):
            if results_stack.currentIndex() != 3:
                results_stack.setCurrentIndex(index)
                for i, btn in enumerate(button_group):
                    btn.setChecked(i == index)
        output_btn.clicked.connect(lambda: switch_results_view(0))
        message_btn.clicked.connect(lambda: switch_results_view(1))
        notification_btn.clicked.connect(lambda: switch_results_view(2))
        main_vertical_splitter.addWidget(results_container)
        main_vertical_splitter.setSizes([300, 300])
        tab_content.setLayout(layout)

        # Insert new worksheet before the Processes tab if it exists
        insert_index = self.tab_widget.count()
        if self.processes_tab:
            insert_index = self.tab_widget.indexOf(self.processes_tab)

        worksheet_count = sum(1 for i in range(self.tab_widget.count()) if not (
            self.processes_tab and self.tab_widget.widget(i) == self.processes_tab))

        index = self.tab_widget.insertTab(
            insert_index, tab_content, f"Worksheet {worksheet_count + 1}")
        self.tab_widget.setCurrentIndex(index)
        return tab_content

    def export_current_results(self):
        current_tab = self.tab_widget.currentWidget()
        if not current_tab:
            return
        table_view = current_tab.findChild(QTableView, "result_table")
        model = table_view.model()
        if not model or model.rowCount() == 0:
            QMessageBox.warning(self, "No Data", "There is no data to export.")
            return
        dialog = ExportDialog(
            self, f"query_results_{datetime.datetime.now().strftime('%Y%m%d')}.csv")
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        options = dialog.get_options()
        file_path = options['filename']
        if not file_path:
            QMessageBox.warning(self, "No Filename",
                                "Export cancelled. No filename specified.")
            return
        try:
            self.status_message_label.setText("Exporting Data")
            QApplication.processEvents()
            columns = [model.headerData(i, Qt.Orientation.Horizontal)
                       for i in range(model.columnCount())]
            data = []
            for row in range(model.rowCount()):
                row_data = [model.data(model.index(row, col))
                            for col in range(model.columnCount())]
                data.append(row_data)
            df = pd.DataFrame(data, columns=columns)

            # MODIFIED: Logic to handle different file formats
            if options['format'] == 'xlsx':
                df.to_excel(file_path, index=False, header=options['header'])
            else: # Default to CSV
                df.to_csv(
                    file_path, index=False, header=options['header'], sep=options['delimiter'],
                    encoding=options['encoding'], quotechar=options['quote']
                )

            QMessageBox.information(
                self, "Success", f"Data successfully exported to:\n{file_path}")
            self.status_message_label.setText(
                f"Exported {len(data)} rows to {os.path.basename(file_path)}")
        except Exception as e:
            QMessageBox.critical(
                self, "Export Error", f"An error occurred while exporting the data:\n{e}")
            self.status_message_label.setText("Export failed.")

    def close_tab(self, index):
        if self.tab_widget.count() <= 1:
            QMessageBox.information(
                self, "Cannot Close", "At least one tab must remain open.")
            return

        tab_to_close = self.tab_widget.widget(index)

        if tab_to_close == self.processes_tab:
            self.processes_tab = None  # Reset the reference if this tab is closed

        if tab_to_close in self.running_queries:
            self.running_queries[tab_to_close].cancel()
            del self.running_queries[tab_to_close]
            if not self.running_queries:
                self.cancel_action.setEnabled(False)

        if tab_to_close in self.tab_timers:
            self.tab_timers[tab_to_close]["timer"].stop()
            if "timeout_timer" in self.tab_timers[tab_to_close]:
                self.tab_timers[tab_to_close]["timeout_timer"].stop()
            del self.tab_timers[tab_to_close]

        self.tab_widget.removeTab(index)
        self.renumber_tabs()

    def renumber_tabs(self):
        worksheet_counter = 1
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            # Only renumber tabs that are not the Processes tab
            if not (self.processes_tab and tab == self.processes_tab):
                self.tab_widget.setTabText(i, f"Worksheet {worksheet_counter}")
                worksheet_counter += 1

    def load_data(self):
        self.model.clear()
        self.model.setHorizontalHeaderLabels(["Object Explorer"])
        hierarchical_data = db.get_hierarchy_data()
        for cat_data in hierarchical_data:
            cat_item = QStandardItem(cat_data['name'])
            cat_item.setData(cat_data['id'], Qt.ItemDataRole.UserRole + 1)
            for subcat_data in cat_data['subcategories']:
                subcat_item = QStandardItem(subcat_data['name'])
                subcat_item.setData(
                    subcat_data['id'], Qt.ItemDataRole.UserRole + 1)
                for item_data in subcat_data['items']:
                    item_item = QStandardItem(item_data['name'])
                    item_item.setData(item_data, Qt.ItemDataRole.UserRole)
                    subcat_item.appendRow(item_item)
                cat_item.appendRow(subcat_item)
            self.model.appendRow(cat_item)

    def item_clicked(self, index):
        item = self.model.itemFromIndex(index)
        depth = self.get_item_depth(item)
        self.schema_model.clear()
        self.schema_model.setHorizontalHeaderLabels(["Database Schema"])
        if depth == 3:
            conn_data = item.data(Qt.ItemDataRole.UserRole)
            if conn_data:
                if conn_data.get("host"):
                    self.status.showMessage(
                        f"Loading schema for {conn_data.get('name')}...", 3000)
                    self.load_postgres_schema(conn_data)
                elif conn_data.get("db_path"):
                    self.status.showMessage(
                        f"Loading schema for {conn_data.get('name')}...", 3000)
                    self.load_sqlite_schema(conn_data)

    def get_item_depth(self, item):
        depth = 0
        parent = item.parent()
        while parent is not None:
            depth += 1
            parent = parent.parent()
        return depth + 1

    def show_context_menu(self, pos):
        index = self.tree.indexAt(pos)
        if not index.isValid():
            return
        item = self.model.itemFromIndex(index)
        depth = self.get_item_depth(item)
        menu = QMenu()
        if depth == 1:
            add_subcat = QAction("Add Group", self)
            add_subcat.triggered.connect(lambda: self.add_subcategory(item))
            menu.addAction(add_subcat)
        elif depth == 2:
            parent_category_item = item.parent()
            if parent_category_item:
                category_name = parent_category_item.text()
                if "postgres" in category_name.lower():
                    add_pg_action = QAction(
                        "Add New PostgreSQL Connection", self)
                    add_pg_action.triggered.connect(
                        lambda: self.add_postgres_connection(item))
                    menu.addAction(add_pg_action)
                elif "sqlite" in category_name.lower():
                    add_sqlite_action = QAction(
                        "Add New SQLite Connection", self)
                    add_sqlite_action.triggered.connect(
                        lambda: self.add_sqlite_connection(item))
                    menu.addAction(add_sqlite_action)
        elif depth == 3:
            conn_data = item.data(Qt.ItemDataRole.UserRole)
            if conn_data:
                if conn_data.get("db_path"):
                    edit_action = QAction("Edit Connection", self)
                    edit_action.triggered.connect(lambda: self.edit_item(item))
                    menu.addAction(edit_action)
                elif conn_data.get("host"):
                    edit_action = QAction("Edit Connection", self)
                    edit_action.triggered.connect(
                        lambda: self.edit_pg_item(item))
                    menu.addAction(edit_action)
                delete_action = QAction("Delete Connection", self)
                delete_action.triggered.connect(lambda: self.delete_item(item))
                menu.addAction(delete_action)
        menu.exec(self.tree.viewport().mapToGlobal(pos))

    def add_subcategory(self, parent_item):
        name, ok = QInputDialog.getText(self, "New Group", "Group name:")
        if ok and name:
            parent_id = parent_item.data(Qt.ItemDataRole.UserRole+1)
            db.add_subcategory(name, parent_id)
            self.load_data()

    def add_postgres_connection(self, parent_item):
        subcat_id = parent_item.data(Qt.ItemDataRole.UserRole + 1)
        dialog = PostgresConnectionDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            try:
                db.add_item(data, subcat_id)
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to save PostgreSQL connection:\n{e}")

    def add_sqlite_connection(self, parent_item):
        subcat_id = parent_item.data(Qt.ItemDataRole.UserRole + 1)
        dialog = SQLiteConnectionDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            data = dialog.get_data()
            try:
                db.add_item(data, subcat_id)
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to save SQLite connection:\n{e}")

    def edit_item(self, item):
        conn_data = item.data(Qt.ItemDataRole.UserRole)
        if conn_data and conn_data.get("db_path"):
            dialog = SQLiteConnectionDialog(self, conn_data=conn_data)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                new_data = dialog.get_data()
                try:
                    db.update_item(new_data)
                    self.load_data()
                    self.refresh_all_comboboxes()
                except Exception as e:
                    QMessageBox.critical(
                        self, "Error", f"Failed to update SQLite connection:\n{e}")

    def edit_pg_item(self, item):
        conn_data = item.data(Qt.ItemDataRole.UserRole)
        if not conn_data:
            return
        dialog = PostgresConnectionDialog(self, is_editing=True)
        dialog.name_input.setText(conn_data.get("name", ""))
        dialog.host_input.setText(conn_data.get("host", ""))
        dialog.port_input.setText(str(conn_data.get("port", "")))
        dialog.db_input.setText(conn_data.get("database", ""))
        dialog.user_input.setText(conn_data.get("user", ""))
        dialog.password_input.setText(conn_data.get("password", ""))
        if dialog.exec() == QDialog.DialogCode.Accepted:
            new_data = dialog.get_data()
            new_data["id"] = conn_data.get("id")
            try:
                db.update_item(new_data)
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to update PostgreSQL connection:\n{e}")

    def delete_item(self, item):
        conn_data = item.data(Qt.ItemDataRole.UserRole)
        item_id = conn_data.get("id")
        reply = QMessageBox.question(self, "Delete Connection", "Are you sure you want to delete this connection?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                db.delete_item(item_id)
                self.load_data()
                self.refresh_all_comboboxes()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to delete item:\n{e}")

    def refresh_all_comboboxes(self):
        for i in range(self.tab_widget.count()):
            tab = self.tab_widget.widget(i)
            if not (self.processes_tab and tab == self.processes_tab):
                combo_box = tab.findChild(QComboBox, "db_combo_box")
                if combo_box:
                    self.load_joined_items(combo_box)

    def load_joined_items(self, combo_box):
        try:
            current_data = combo_box.currentData()
            combo_box.clear()
            all_items = db.get_all_connections_from_db()
            for item in all_items:
                conn_data = {key: item[key]
                             for key in item if key != 'display_name'}
                combo_box.addItem(item["display_name"], conn_data)
            if current_data:
                for i in range(combo_box.count()):
                    if combo_box.itemData(i) and combo_box.itemData(i)['id'] == current_data['id']:
                        combo_box.setCurrentIndex(i)
                        break
        except Exception as e:
            self.status.showMessage(f"Error loading connections: {e}", 4000)

    def execute_query(self):
        current_tab = self.tab_widget.currentWidget()
        if not current_tab:
            return
        editor_stack = current_tab.findChild(QStackedWidget, "editor_stack")
        if editor_stack and editor_stack.currentIndex() == 1:
            QMessageBox.information(
                self, "Info", "Cannot execute from History view. Switch to the Query view.")
            return
        if current_tab in self.running_queries:
            QMessageBox.warning(self, "Query in Progress",
                                "A query is already running in this tab.")
            return
        query_editor = current_tab.findChild(QTextEdit, "query_editor")
        db_combo_box = current_tab.findChild(QComboBox, "db_combo_box")
        conn_data = db_combo_box.currentData()
        query = query_editor.toPlainText().strip()
        if not conn_data or not query:
            self.status.showMessage("Connection or query is empty", 3000)
            return
        results_stack = current_tab.findChild(
            QStackedWidget, "results_stacked_widget")
        spinner_label = results_stack.findChild(QLabel, "spinner_label")
        results_stack.setCurrentIndex(3)
        if spinner_label and spinner_label.movie():
            spinner_label.movie().start()
        tab_status_label = current_tab.findChild(QLabel, "tab_status_label")
        progress_timer = QTimer(self)
        start_time = time.time()
        timeout_timer = QTimer(self)
        timeout_timer.setSingleShot(True)
        self.tab_timers[current_tab] = {
            "timer": progress_timer, "start_time": start_time, "timeout_timer": timeout_timer}
        progress_timer.timeout.connect(
            partial(self.update_timer_label, tab_status_label, current_tab))
        progress_timer.start(100)
        signals = QuerySignals()
        runnable = RunnableQuery(conn_data, query, signals)
        signals.finished.connect(
            partial(self.handle_query_result, current_tab))
        signals.error.connect(partial(self.handle_query_error, current_tab))
        # timeout_timer.timeout.connect(
        #     partial(self.handle_query_timeout, current_tab, runnable))
        self.running_queries[current_tab] = runnable
        self.cancel_action.setEnabled(True)
        self.thread_pool.start(runnable)
        # timeout_timer.start(self.QUERY_TIMEOUT)
        # self.status_message_label.setText("Executing query...")

    def update_timer_label(self, label, tab):
        if not label or tab not in self.tab_timers:
            return

        elapsed_seconds = time.time() - self.tab_timers[tab]["start_time"]

        minutes, seconds_with_ms = divmod(elapsed_seconds, 60)
        hours, minutes = divmod(minutes, 60)

        seconds_int = int(seconds_with_ms)
        milliseconds = int((seconds_with_ms - seconds_int) * 1000)

        time_str = f"{hours:02.0f}:{minutes:02.0f}:{seconds_int:02d}.{milliseconds:03d}"

        label.setText(f"Running... {time_str}")
        # elapsed = time.time() - self.tab_timers[tab]["start_time"]
        # label.setText(f"Running... {elapsed:.1f} sec")

    def format_duration_ms(self, total_seconds):
        """Converts seconds into HH:MM:SS.ms format. This function is well-written."""
        if total_seconds is None:
            return "00:00:00.000"

        seconds_int = int(total_seconds)
        milliseconds = int((total_seconds - seconds_int) * 1000)

        minutes, seconds = divmod(seconds_int, 60)
        hours, minutes = divmod(minutes, 60)

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"

    def handle_query_error(self, target_tab, error_message):
        if target_tab in self.tab_timers:
            self.tab_timers[target_tab]["timer"].stop()
            self.tab_timers[target_tab]["timeout_timer"].stop()
            del self.tab_timers[target_tab]
        message_view = target_tab.findChild(QTextEdit, "message_view")
        tab_status_label = target_tab.findChild(QLabel, "tab_status_label")
        error_text = f"Error: {error_message}"
        message_view.setText(f"Error:\n\n{error_message}")
        tab_status_label.setText(error_text)
        self.status_message_label.setText("Error occurred")
        self.stop_spinner(target_tab, success=False)
        if target_tab in self.running_queries:
            del self.running_queries[target_tab]
        if not self.running_queries:
            self.cancel_action.setEnabled(False)

    def stop_spinner(self, target_tab, success=True):
        if not target_tab:
            return
        stacked_widget = target_tab.findChild(
            QStackedWidget, "results_stacked_widget")
        if stacked_widget:
            spinner_label = stacked_widget.findChild(QLabel, "spinner_label")
            if spinner_label and spinner_label.movie():
                spinner_label.movie().stop()
            header = target_tab.findChild(QWidget, "resultsHeader")
            buttons = header.findChildren(QPushButton)
            if success:
                stacked_widget.setCurrentIndex(0)
                if buttons:
                    buttons[0].setChecked(True)
                    buttons[1].setChecked(False)
                    buttons[2].setChecked(False)
            else:
                stacked_widget.setCurrentIndex(1)
                if buttons:
                    buttons[0].setChecked(False)
                    buttons[1].setChecked(True)
                    buttons[2].setChecked(False)

    def handle_query_result(self, target_tab, conn_data, query, results, columns, row_count, elapsed_time, is_select_query):
        if target_tab in self.tab_timers:
            self.tab_timers[target_tab]["timer"].stop()
            self.tab_timers[target_tab]["timeout_timer"].stop()
            del self.tab_timers[target_tab]

        self.save_query_to_history(
            conn_data, query, "Success", row_count, elapsed_time)

        table_view = target_tab.findChild(QTableView, "result_table")
        message_view = target_tab.findChild(QTextEdit, "message_view")
        tab_status_label = target_tab.findChild(QLabel, "tab_status_label")

        formatted_time = self.format_duration_ms(elapsed_time)

        if is_select_query:
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(columns)
            for row in results:
                model.appendRow([QStandardItem(str(cell)) for cell in row])
            table_view.setModel(model)

            msg = f"Query executed successfully.\n\nTotal rows: {row_count}\nTime: {formatted_time}"
            status = f"Query executed successfully | Total rows: {row_count} | Query complete {formatted_time}"
        else:
            table_view.setModel(QStandardItemModel())

            msg = f"Command executed successfully.\n\nRows affected: {row_count}\nTime: {formatted_time}"
            status = f"Command executed successfully | Rows affected: {row_count} | Query complete {formatted_time}"

        message_view.setText(msg)
        tab_status_label.setText(status)
        self.status_message_label.setText("Ready")
        self.stop_spinner(target_tab, success=True)

        if target_tab in self.running_queries:
            del self.running_queries[target_tab]
        if not self.running_queries:
            self.cancel_action.setEnabled(False)

    def handle_query_timeout(self, tab, runnable):
        if self.running_queries.get(tab) is runnable:
            runnable.cancel()
            error_message = f"Error: Query Timed Out after {self.QUERY_TIMEOUT / 1000} seconds."
            tab.findChild(QTextEdit, "message_view").setText(error_message)
            tab.findChild(QLabel, "tab_status_label").setText(error_message)
            self.stop_spinner(tab, success=False)
            if tab in self.tab_timers:
                self.tab_timers[tab]["timer"].stop()
                del self.tab_timers[tab]
            if tab in self.running_queries:
                del self.running_queries[tab]
            if not self.running_queries:
                self.cancel_action.setEnabled(False)
            self.status_message_label.setText("Error occurred")
            QMessageBox.warning(
                self, "Query Timeout", f"The query was stopped as it exceeded {self.QUERY_TIMEOUT / 1000}s.")

    def cancel_current_query(self):
        current_tab = self.tab_widget.currentWidget()
        runnable = self.running_queries.get(current_tab)
        if runnable:
            runnable.cancel()
            if current_tab in self.tab_timers:
                self.tab_timers[current_tab]["timer"].stop()
                self.tab_timers[current_tab]["timeout_timer"].stop()
                del self.tab_timers[current_tab]
            cancel_message = "Query cancelled by user."
            current_tab.findChild(
                QTextEdit, "message_view").setText(cancel_message)
            current_tab.findChild(
                QLabel, "tab_status_label").setText(cancel_message)
            self.stop_spinner(current_tab, success=False)
            self.status_message_label.setText("Query Cancelled")
            if current_tab in self.running_queries:
                del self.running_queries[current_tab]
            if not self.running_queries:
                self.cancel_action.setEnabled(False)

    def save_query_to_history(self, conn_data, query, status, rows, duration):
        conn_id = conn_data.get("id")
        if not conn_id:
            return
        try:
            db.save_query_history(conn_id, query, status, rows, duration)
        except Exception as e:
            self.status.showMessage(
                f"Could not save query to history: {e}", 4000)

    def load_connection_history(self, target_tab):
        history_list_view = target_tab.findChild(
            QTreeView, "history_list_view")
        history_details_view = target_tab.findChild(
            QTextEdit, "history_details_view")
        db_combo_box = target_tab.findChild(QComboBox, "db_combo_box")
        model = QStandardItemModel()
        model.setHorizontalHeaderLabels(['Connection History'])
        history_list_view.setModel(model)
        history_details_view.clear()
        conn_data = db_combo_box.currentData()
        if not conn_data:
            return
        conn_id = conn_data.get("id")
        try:
            history = db.get_query_history(conn_id)
            for row in history:
                history_id, query, ts, status, rows, duration = row
                short_query = ' '.join(query.split())[
                    :70] + ('...' if len(query) > 70 else '')
                dt = datetime.datetime.fromisoformat(ts)
                display_text = f"{short_query}\n{dt.strftime('%Y-%m-%d %H:%M:%S')}"
                item = QStandardItem(display_text)
                item.setData({"id": history_id, "query": query, "timestamp": dt.strftime(
                    '%Y-%m-%d %H:%M:%S'), "status": status, "rows": rows, "duration": f"{duration:.3f} sec"}, Qt.ItemDataRole.UserRole)
                model.appendRow(item)
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Failed to load query history:\n{e}")

    def display_history_details(self, index, target_tab):
        history_details_view = target_tab.findChild(
            QTextEdit, "history_details_view")
        if not index.isValid() or not history_details_view:
            return
        data = index.model().itemFromIndex(index).data(Qt.ItemDataRole.UserRole)
        details_text = f"Timestamp: {data['timestamp']}\nStatus: {data['status']}\nDuration: {data['duration']}\nRows: {data['rows']}\n\n-- Query --\n{data['query']}"
        history_details_view.setText(details_text)

    def _get_selected_history_item(self, target_tab):
        history_list_view = target_tab.findChild(
            QTreeView, "history_list_view")
        selected_indexes = history_list_view.selectionModel().selectedIndexes()
        if not selected_indexes:
            QMessageBox.information(
                self, "No Selection", "Please select a history item first.")
            return None
        item = selected_indexes[0].model().itemFromIndex(selected_indexes[0])
        return item.data(Qt.ItemDataRole.UserRole)

    def copy_history_query(self, target_tab):
        history_data = self._get_selected_history_item(target_tab)
        if history_data:
            QApplication.clipboard().setText(history_data['query'])
            self.status_message_label.setText("Query copied to clipboard.")

    def copy_history_to_editor(self, target_tab):
        history_data = self._get_selected_history_item(target_tab)
        if history_data:
            editor_stack = target_tab.findChild(QStackedWidget, "editor_stack")
            query_editor = target_tab.findChild(QTextEdit, "query_editor")
            query_editor.setPlainText(history_data['query'])
            editor_stack.setCurrentIndex(0)
            query_view_btn = target_tab.findChild(QPushButton, "Query")
            history_view_btn = target_tab.findChild(
                QPushButton, "Query History")
            if query_view_btn:
                query_view_btn.setChecked(True)
            if history_view_btn:
                history_view_btn.setChecked(False)
            self.status_message_label.setText("Query copied to editor.")

    def remove_selected_history(self, target_tab):
        history_data = self._get_selected_history_item(target_tab)
        if not history_data:
            return
        history_id = history_data['id']
        reply = QMessageBox.question(self, "Remove History", "Are you sure you want to remove the selected query history?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                db.delete_history_item(history_id)
                self.load_connection_history(target_tab)
                target_tab.findChild(QTextEdit, "history_details_view").clear()
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to remove history item:\n{e}")

    def remove_all_history_for_connection(self, target_tab):
        db_combo_box = target_tab.findChild(QComboBox, "db_combo_box")
        conn_data = db_combo_box.currentData()
        if not conn_data:
            QMessageBox.warning(self, "No Connection",
                                "Please select a connection first.")
            return
        conn_id = conn_data.get("id")
        conn_name = db_combo_box.currentText()
        reply = QMessageBox.question(
            self, "Remove All History", f"Are you sure you want to remove all history for the connection:\n'{conn_name}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                db.delete_all_history_for_connection(conn_id)
                self.load_connection_history(target_tab)
            except Exception as e:
                QMessageBox.critical(
                    self, "Error", f"Failed to clear history for this connection:\n{e}")
     # last change menu

    def load_sqlite_schema(self, conn_data):
        self.schema_model.clear()
        # হেডার পরিবর্তন করে দুটি কলাম যুক্ত করা হয়েছে
        self.schema_model.setHorizontalHeaderLabels(["Name", "Type"])
        db_path = conn_data.get("db_path")
        if not db_path or not os.path.exists(db_path):
            self.status.showMessage(
                f"Error: SQLite DB path not found: {db_path}", 5000)
            return
        try:
            conn = sqlite.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name;")
            tables = cursor.fetchall()
            conn.close()
            for name, type_str in tables:
                icon = QIcon(
                    "assets/table_icon.png") if type_str == 'table' else QIcon("assets/view_icon.png")

                name_item = QStandardItem(icon, name)
                name_item.setEditable(False)
                name_item.setData(
                    {'db_type': 'sqlite', 'conn_data': conn_data}, Qt.ItemDataRole.UserRole)

                type_item = QStandardItem(type_str.capitalize())
                type_item.setEditable(False)

                self.schema_model.appendRow([name_item, type_item])

            if hasattr(self, '_expanded_connection'):
                try:
                    self.schema_tree.expanded.disconnect(
                        self._expanded_connection)
                except TypeError:
                    pass
        except Exception as e:
            self.status.showMessage(f"Error loading SQLite schema: {e}", 5000)

    # last change 2
    def load_postgres_schema(self, conn_data):
        try:
            self.schema_model.clear()
            # হেডার পরিবর্তন করে দুটি কলাম যুক্ত করা হয়েছে
            self.schema_model.setHorizontalHeaderLabels(["Name", "Type"])
            self.pg_conn = psycopg2.connect(host=conn_data["host"], database=conn_data["database"],
                                            user=conn_data["user"], password=conn_data["password"], port=int(conn_data["port"]))
            cursor = self.pg_conn.cursor()
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') ORDER BY schema_name;")
            schemas = cursor.fetchall()
            # প্রতিটি স্কিমার জন্য নাম এবং তার ধরণসহ একটি সারি যুক্ত করা হয়েছে
            for (schema_name,) in schemas:
                schema_item = QStandardItem(
                    QIcon("assets/schema_icon.png"), schema_name)
                schema_item.setEditable(False)
                item_data = {'db_type': 'postgres',
                             'schema_name': schema_name, 'conn_data': conn_data}
                schema_item.setData(item_data, Qt.ItemDataRole.UserRole)
                schema_item.appendRow(QStandardItem("Loading..."))

                type_item = QStandardItem("Schema")
                type_item.setEditable(False)

                self.schema_model.appendRow([schema_item, type_item])

            if hasattr(self, '_expanded_connection'):
                try:
                    self.schema_tree.expanded.disconnect(
                        self._expanded_connection)
                except TypeError:
                    pass
            self._expanded_connection = self.schema_tree.expanded.connect(
                self.load_tables_on_expand)
        except Exception as e:
            self.status.showMessage(f"Error loading schemas: {e}", 5000)
            if hasattr(self, 'pg_conn') and self.pg_conn:
                self.pg_conn.close()

    def show_schema_context_menu(self, position):
        index = self.schema_tree.indexAt(position)
        if not index.isValid():
            return
        item = self.schema_model.itemFromIndex(index)
        item_data = item.data(Qt.ItemDataRole.UserRole)
        is_table = item_data and (item_data.get('db_type') == 'sqlite' or (
            item.parent() and item_data.get('db_type') == 'postgres'))
        if not is_table:
            return
        table_name = item.text()
        menu = QMenu()
        view_menu = menu.addMenu("View/Edit Data")
        query_all_action = QAction("All Rows", self)
        query_all_action.triggered.connect(lambda: self.query_table_rows(
            item_data, table_name, limit=None, execute_now=True))
        view_menu.addAction(query_all_action)
        preview_100_action = QAction("First 100 Rows", self)
        preview_100_action.triggered.connect(lambda: self.query_table_rows(
            item_data, table_name, limit=100, execute_now=True))
        view_menu.addAction(preview_100_action)
        last_100_action = QAction("Last 100 Rows", self)
        last_100_action.triggered.connect(lambda: self.query_table_rows(
            item_data, table_name, limit=100, order='desc', execute_now=True))
        view_menu.addAction(last_100_action)
        # add count
        count_rows_action = QAction("Count Rows", self)
        count_rows_action.triggered.connect(
            lambda: self.count_table_rows(item_data, table_name)
        )
        view_menu.addAction(count_rows_action)
        menu.addSeparator()
        query_tool_action = QAction("Query Tool", self)
        query_tool_action.triggered.connect(
            lambda: self.open_query_tool_for_table(item_data, table_name))
        menu.addAction(query_tool_action)
        view_menu.addSeparator()
        export_rows_action = QAction("Export Rows", self)
        export_rows_action.triggered.connect(
            lambda: self.export_schema_table_rows(item_data, table_name))
        menu.addAction(export_rows_action)

        # "Properties" অপশন যোগ করা হয়েছে
        properties_action = QAction("Properties", self)
        properties_action.triggered.connect(
            lambda: self.show_table_properties(item_data, table_name))
        menu.addAction(properties_action)

        menu.exec(self.schema_tree.viewport().mapToGlobal(position))
        menu.exec(self.schema_tree.viewport().mapToGlobal(position))

    def show_table_properties(self, item_data, table_name):
        dialog = TablePropertiesDialog(item_data, table_name, self)
        dialog.exec()
    # <<< MODIFIED >>> This function now creates and opens the Processes tab.

    def export_schema_table_rows(self, item_data, table_name):
        if not item_data:
            return

        default_filename = f"{table_name}_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
        dialog = ExportDialog(self, default_filename)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return

        # Ensure the Processes tab is created and visible
        self._create_processes_tab()

        options = dialog.get_options()
        if not options['filename']:
            QMessageBox.warning(self, "No Filename",
                                "Export cancelled. No filename specified.")
            return

        process_id = str(uuid.uuid4())
        conn_data = item_data['conn_data']
        object_name = f"{item_data.get('schema_name', 'public')}.{table_name}"

        initial_data = {
            "pid": process_id[:8],
            "type": "Export Data",
            "status": "Running",
            "server": conn_data['name'],
            "object": object_name,
            "time_taken": "...",
            "start_time": datetime.datetime.now().strftime("%Y-%m-%d, %I:%M:%S %p"),
            "details": f"Exporting to {os.path.basename(options['filename'])}"
        }

        signals = ProcessSignals()
        signals.started.connect(self.handle_process_started)
        signals.finished.connect(self.handle_process_finished)
        signals.error.connect(self.handle_process_error)

        # Immediately add to table
        signals.started.emit(process_id, initial_data)

        runnable = RunnableExport(
            process_id, item_data, table_name, options, signals)
        self.thread_pool.start(runnable)

    # Handlers for background process signals
    def handle_process_started(self, process_id, data):
        # Switch to the processes tab when a process starts
        if self.processes_tab:
            self.tab_widget.setCurrentWidget(self.processes_tab)

        row_items = []
        for key in ["pid", "type", "status", "server", "object", "time_taken", "start_time", "details"]:
            item = QStandardItem(data[key])
            if key == "pid":
                # Store full ID
                item.setData(process_id, Qt.ItemDataRole.UserRole)
            if key == "status":
                item.setIcon(QIcon("assets/running_icon.png"))
            row_items.append(item)

        self.processes_model.appendRow(row_items)

    def find_process_row(self, process_id):
        for row in range(self.processes_model.rowCount()):
            item = self.processes_model.item(row, 0)  # PID is in column 0
            if item and item.data(Qt.ItemDataRole.UserRole) == process_id:
                return row
        return -1

    def handle_process_finished(self, process_id, message, time_taken):
        row = self.find_process_row(process_id)
        if row == -1:
            return

        # Update Status
        status_item = QStandardItem("Finished")
        status_item.setBackground(QBrush(QColor("#d4edda")))
        status_item.setIcon(QIcon("assets/finished_icon.png"))
        self.processes_model.setItem(row, 2, status_item)

        # Update Time Taken
        self.processes_model.item(row, 5).setText(f"{time_taken:.2f}")
        # Update Details
        self.processes_model.item(row, 7).setText(message)

    def handle_process_error(self, process_id, error_message):
        row = self.find_process_row(process_id)
        if row == -1:
            return

        # Update Status
        status_item = QStandardItem("Error")
        status_item.setBackground(QBrush(QColor("#f8d7da")))
        status_item.setIcon(QIcon("assets/error_icon.png"))
        self.processes_model.setItem(row, 2, status_item)

        # Update Details
        self.processes_model.item(row, 7).setText(error_message)
    # count row1

    def count_table_rows(self, item_data, table_name):
        if not item_data:
            return

        conn_data = item_data.get('conn_data')

        if item_data.get('db_type') == 'postgres':
            query = f'SELECT COUNT(*) FROM "{item_data.get("schema_name")}"."{table_name}";'
        else:  # Handles SQLite
            query = f'SELECT COUNT(*) FROM "{table_name}";'

        self.status_message_label.setText(f"Counting rows for {table_name}...")

        signals = QuerySignals()
        runnable = RunnableQuery(conn_data, query, signals)

        # Connect to a specific handler for this action
        signals.finished.connect(self.handle_count_result)
        signals.error.connect(self.handle_count_error)

        self.thread_pool.start(runnable)

    def handle_count_result(self, conn_data, query, results, columns, row_count, elapsed_time, is_select_query):
        try:
            if results and len(results) > 0 and len(results[0]) > 0:
                count = results[0][0]
                message = f"Table rows counted: {count}"
                #  CHANGE: Use the manager to show the message.
                self.notification_manager.show_message(message)
                self.status_message_label.setText(
                    f"Successfully counted rows in {elapsed_time:.2f} sec."
                )
            else:
                self.handle_count_error("Could not retrieve count.")
        except Exception as e:
            self.handle_count_error(str(e))

    def handle_count_error(self, error_message):
        # CHANGE: Use the manager to show the error message.
        self.notification_manager.show_message(
            f"Error: {error_message}", is_error=True
        )
        self.status_message_label.setText("Failed to count rows.")

        # end

    def open_query_tool_for_table(self, item_data, table_name):
        self.query_table_rows(item_data, table_name, execute_now=False)

    def query_table_rows(self, item_data, table_name, limit=None, execute_now=True, order=None):
        if not item_data:
            return
        conn_data = item_data.get('conn_data')
        new_tab = self.add_tab()
        query_editor = new_tab.findChild(QTextEdit, "query_editor")
        db_combo_box = new_tab.findChild(QComboBox, "db_combo_box")
        for i in range(db_combo_box.count()):
            data = db_combo_box.itemData(i)
            if data and data.get('id') == conn_data.get('id'):
                db_combo_box.setCurrentIndex(i)
                break
        if item_data.get('db_type') == 'postgres':
            query = f'SELECT * FROM "{item_data.get("schema_name")}"."{table_name}"'
        else:
            query = f'SELECT * FROM "{table_name}"'
        if order:
            query += f" ORDER BY 1 {order.upper()}"
        if limit:
            query += f" LIMIT {limit}"
        query_editor.setPlainText(query)
        if execute_now:
            self.tab_widget.setCurrentWidget(new_tab)
            self.execute_query()
    # last change 3

    def load_tables_on_expand(self, index: QModelIndex):
        item = self.schema_model.itemFromIndex(index)
        if not item or (item.rowCount() > 0 and item.child(0).text() != "Loading..."):
            return
        item.removeRows(0, item.rowCount())
        item_data = item.data(Qt.ItemDataRole.UserRole)
        schema_name = item_data.get('schema_name')
        try:
            cursor = self.pg_conn.cursor()
            cursor.execute(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = %s ORDER BY table_type, table_name;", (schema_name,))
            tables = cursor.fetchall()

            for (table_name, table_type) in tables:
                icon_path = "assets/table_icon.png" if "TABLE" in table_type else "assets/view_icon.png"
                display_type = "Table" if "TABLE" in table_type else "View"

                table_item = QStandardItem(QIcon(icon_path), table_name)
                table_item.setEditable(False)
                table_item.setData(item_data, Qt.ItemDataRole.UserRole)

                type_item = QStandardItem(display_type)
                type_item.setEditable(False)

                item.appendRow([table_item, type_item])
        except Exception as e:
            self.status.showMessage(f"Error expanding schema: {e}", 5000)

