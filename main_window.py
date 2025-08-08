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
    QStackedWidget, QLabel, QGroupBox, QDialogButtonBox, QCheckBox, QRadioButton, QStyle
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
    A custom widget for displaying pgAdmin-style notifications.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | Qt.WindowType.ToolTip)
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
        self.close_button.clicked.connect(self.hide)

        layout.addWidget(self.icon_label)
        layout.addWidget(self.message_label)
        layout.addStretch()
        layout.addWidget(self.close_button)

        self.hide_timer = QTimer(self)
        self.hide_timer.setSingleShot(True)
        self.hide_timer.timeout.connect(self.hide)

    def show_message(self, message, is_error=False, duration=5000):
        self.message_label.setText(message)

        if is_error:
            self.setProperty("isError", True)
            icon = self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxCritical)
        else:
            self.setProperty("isError", False)
            icon = self.style().standardIcon(QStyle.StandardPixmap.SP_DialogApplyButton)

        self.icon_label.setPixmap(icon.pixmap(16, 16))

        # Refresh style
        self.style().unpolish(self)
        self.style().polish(self)

        self.show()
        self.adjustSize()

        # Position at bottom-right of parent
        if self.parentWidget():
            parent_rect = self.parentWidget().geometry()
            status_bar_height = self.parentWidget().statusBar().height(
            ) if hasattr(self.parentWidget(), 'statusBar') else 0
            x = parent_rect.width() - self.width() - 15
            y = parent_rect.height() - self.height() - status_bar_height - 15
            self.move(x, y)

        self.hide_timer.start(duration)
# pgAdmin-style Export Dialog with Tabs


class ExportDialog(QDialog):
    """
    A custom dialog to get options for exporting data, with tabs like pgAdmin.
    """

    def __init__(self, parent=None, default_filename="export.csv"):
        super().__init__(parent)
        self.setWindowTitle("Import/Export data")
        self.setMinimumWidth(550)

        main_layout = QVBoxLayout(self)

        # Main Tab Widget
        tab_widget = QTabWidget()
        main_layout.addWidget(tab_widget)

        # Create Tabs
        general_tab = QWidget()
        options_tab = QWidget()

        tab_widget.addTab(general_tab, "General")
        tab_widget.addTab(options_tab, "Options")

        # --- General Tab Layout ---
        general_layout = QFormLayout(general_tab)

        # Import/Export Switch
        self.export_radio = QRadioButton("Export")
        self.import_radio = QRadioButton("Import")
        self.export_radio.setChecked(True)
        self.import_radio.setEnabled(False)  # Import not implemented
        switch_layout = QHBoxLayout()
        switch_layout.addWidget(self.export_radio)
        switch_layout.addWidget(self.import_radio)
        general_layout.addRow("Import/Export:", switch_layout)

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
        self.format_combo.addItems(["csv", "binary", "text"])
        self.format_combo.setCurrentText("csv")
        general_layout.addRow("Format:", self.format_combo)

        # Encoding
        self.encoding_combo = QComboBox()
        self.encoding_combo.addItems(['UTF-8', 'LATIN1', 'windows-1252'])
        self.encoding_combo.setEditable(True)
        general_layout.addRow("Encoding:", self.encoding_combo)

        # --- Options Tab Layout ---
        options_layout = QFormLayout(options_tab)

        self.header_check = QCheckBox("Header")
        self.header_check.setChecked(True)
        options_layout.addRow("Options:", self.header_check)

        self.delimiter_combo = QComboBox()
        self.delimiter_combo.addItems([',', ';', '|', '\\t'])
        self.delimiter_combo.setEditable(True)
        options_layout.addRow("Delimiter:", self.delimiter_combo)

        self.quote_edit = QLineEdit('"')
        self.quote_edit.setMaxLength(1)
        options_layout.addRow("Quote character:", self.quote_edit)

        # OK and Cancel buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        main_layout.addWidget(button_box)

    def browse_file(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Select Output File", self.filename_edit.text(), "CSV Files (*.csv);;All Files (*)")
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

# Signals class for QRunnable worker


class QuerySignals(QObject):
    finished = pyqtSignal(dict, str, list, list, int, float, bool)
    error = pyqtSignal(str)


# Signals for the Background Process Worker
class ProcessSignals(QObject):
    started = pyqtSignal(str, dict)  # process_id, initial_data
    finished = pyqtSignal(str, str, float)  # process_id, message, time_taken
    error = pyqtSignal(str, str)  # process_id, error_message


# Worker for Asynchronous Export
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
        self.notification_widget = NotificationWidget(self)
        self.notification_widget.hide()
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
        # Color palette: #D3D3D3 (LightGray), #ADD8E6 (LightBlue), #C0C0C0 (Silver)
        primary_color = "#D3D3D3"      # LightGray for backgrounds
        header_color = "#ADD8E6"       # LightBlue for headers
        selection_color = "#C0C0C0"    # Silver for selections and borders
        text_color_on_dark = "#FFFFFF"  # White text on dark headers
        text_color_on_light = "#000000"  # Black text on light backgrounds
        alternate_row_color = "#E8E8E8"

        style_sheet = f"""
            QMainWindow, QToolBar, QStatusBar {{
                background-color: {primary_color};
                color: {text_color_on_light};
            }}
            QToolBar QToolButton {{
                background-color: {header_color};
                color: {text_color_on_light};
                padding: 5px;
                border-radius: 3px;
                margin: 2px;
            }}
            QToolBar QToolButton:hover {{
                background-color: #B0E0E6;
            }}
            QTreeView {{
                background-color: white;
                alternate-background-color: {alternate_row_color};
                border: 1px solid {selection_color};
            }}
            QTableView {{
                alternate-background-color: {alternate_row_color};
                background-color: white;
                gridline-color: #d0d0d0;
                border: 1px solid {selection_color};
                font-family: Arial, sans-serif;
                font-size: 9pt;
            }}
            QTableView::item {{ 
                padding: 4px; 
            }}
            QTableView::item:selected {{ 
                background-color: {selection_color}; 
                color: {text_color_on_light}; 
            }}
            QHeaderView::section {{
                background-color: {header_color};
                color: {text_color_on_light};
                padding: 6px;
                border: 1px solid {selection_color};
                font-weight: bold;
                font-size: 9pt;
            }}
            QTableView QTableCornerButton::section {{
                background-color: {header_color};
                border: 1px solid {selection_color};
            }}
            #resultsHeader QPushButton, #editorHeader QPushButton {{
                background-color: #ffffff;
                border: 1px solid {selection_color};
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
            }}
            #resultsHeader, #editorHeader {{
                background-color: {alternate_row_color};
                padding-bottom: -1px;
            }}
            #messageView, #history_details_view, QTextEdit {{
                font-family: Consolas, monospace;
                font-size: 10pt;
                background-color: white;
                border: 1px solid {selection_color};
            }}
            #tab_status_label {{
                padding: 3px 5px;
                background-color: {alternate_row_color};
                border-top: 1px solid {selection_color};
            }}
            QGroupBox {{
                font-size: 9pt;
                font-weight: bold;
                color: {text_color_on_light};
            }}
            QTabWidget::pane {{
                border-top: 1px solid {selection_color};
            }}
            QTabBar::tab {{
                background: {primary_color};
                border: 1px solid {selection_color};
                padding: 5px 10px;
                border-bottom: none;
            }}
            QTabBar::tab:selected {{
                background: {selection_color};
            }}
            QComboBox {{
                border: 1px solid {selection_color};
                padding: 2px;
                background-color: white;
            }}
            #change for count
            /* ADD THESE NEW STYLES FOR NOTIFICATION WIDGET */
            #notificationWidget {{
                background-color: #d4edda; /* Green for success */
                color: #155724;
                border: 1px solid #c3e6cb;
                border-radius: 5px;
                font-size: 9pt;
            }}
            #notificationWidget[isError="true"] {{
                background-color: #f8d7da; /* Red for error */
                color: #721c24;
                border: 1px solid #f5c6cb;
            }}
            #notificationCloseButton {{
                background: transparent;
                border: none;
                font-weight: bold;
                color: #555;
            }}
            #notificationCloseButton:hover {{
                color: #000;
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
        text_edit.setPlaceholderText("Write your SQL query here...")
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
        loading_text_label = QLabel("Waiting for query to complete...")
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
            self.status_message_label.setText("Exporting data...")
            QApplication.processEvents()
            columns = [model.headerData(i, Qt.Orientation.Horizontal)
                       for i in range(model.columnCount())]
            data = []
            for row in range(model.rowCount()):
                row_data = [model.data(model.index(row, col))
                            for col in range(model.columnCount())]
                data.append(row_data)
            df = pd.DataFrame(data, columns=columns)
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
        menu.exec(self.schema_tree.viewport().mapToGlobal(position))

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
                self.notification_widget.show_message(message)
                self.status_message_label.setText(
                    f"Successfully counted rows in {elapsed_time:.2f} sec.")
            else:
                self.handle_count_error("Could not retrieve count.")
        except Exception as e:
            self.handle_count_error(str(e))

    def handle_count_error(self, error_message):
        self.notification_widget.show_message(
            f"Error: {error_message}", is_error=True)
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
