import os
import sys
import csv
import sqlite3
import zlib
import hashlib
import json
from datetime import datetime
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
                           QTableView, QPushButton, QLabel, QLineEdit, QMessageBox,
                           QFileDialog, QComboBox, QTabWidget, QTextEdit, QSplitter,
                           QHeaderView, QAbstractItemView, QMenu, QAction, QInputDialog,
                           QDialog, QFormLayout, QDialogButtonBox, QCheckBox, QStatusBar,
                           QToolBar, QSystemTrayIcon, QMenuBar, QStyle, QToolButton,
                           QListWidget, QListWidgetItem, QStackedWidget, QGroupBox,
                           QSpinBox, QDoubleSpinBox, QDateEdit, QDateTimeEdit, QTextBrowser,
                           QProgressDialog, QSplashScreen, QGraphicsView, QGraphicsScene,
                           QGraphicsRectItem, QGraphicsTextItem, QColorDialog, QCompleter)
from PyQt5.QtGui import (QIcon, QStandardItemModel, QStandardItem, QFont, QColor, 
                        QTextCursor, QSyntaxHighlighter, QTextCharFormat, QKeySequence,
                        QTextDocument, QPixmap, QBrush, QPen, QPainter, QLinearGradient,
                        QPalette, QTextOption, QFontMetrics)
from PyQt5.QtCore import (Qt, QSize, QSettings, QFileInfo, QRegularExpression, 
                         QSortFilterProxyModel, QTimer, pyqtSignal, QThread, QObject,
                         QStringListModel, QRectF, QPointF, QDateTime, QCoreApplication)


class ProjectInfo:
    """项目信息元数据（集中管理所有项目相关信息）"""
    VERSION = "3.0.0"
    BUILD_DATE = "2025-05-26"
    AUTHOR = "杜玛"
    LICENSE = "MIT"
    COPYRIGHT = "© 永久 杜玛"
    URL = "https://github.com/duma520"
    MAINTAINER_EMAIL = "support@duma520.com"
    NAME = "SQLite 数据库管理器 Pro"
    DESCRIPTION = "增强版 SQLite 数据库管理器，支持多种高级功能"
    HELP_TEXT = """
使用说明:

1. 文件菜单:
   - 打开/关闭数据库
   - 备份/恢复数据库
   - 导入/导出数据
   - 多数据库管理

2. 编辑菜单:
   - SQL查询历史
   - 数据编辑功能

3. 视图菜单:
   - 自定义界面布局
   - 切换主题
   - 数据可视化

4. 工具菜单:
   - 数据库优化
   - 完整性检查
   - 加密/解密

5. 帮助菜单:
   - 查看帮助文档
   - 检查更新
"""

    @classmethod
    def get_metadata(cls) -> dict:
        """获取主要元数据字典"""
        return {
            'version': cls.VERSION,
            'author': cls.AUTHOR,
            'license': cls.LICENSE,
            'url': cls.URL
        }

    @classmethod
    def get_header(cls) -> str:
        """生成标准化的项目头信息"""
        return f"{cls.NAME} {cls.VERSION} | {cls.LICENSE} License | {cls.URL}"


class DatabaseBackupThread(QThread):
    """数据库备份线程"""
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(bool, str)

    def __init__(self, source_path, dest_path, parent=None):
        super().__init__(parent)
        self.source_path = source_path
        self.dest_path = dest_path
        self.canceled = False

    def run(self):
        try:
            # 创建备份文件
            if os.path.exists(self.dest_path):
                os.remove(self.dest_path)

            # 连接源数据库
            source_conn = sqlite3.connect(self.source_path)
            source_conn.execute("BEGIN IMMEDIATE")
            
            # 连接目标数据库
            dest_conn = sqlite3.connect(self.dest_path)
            
            # 备份数据
            with dest_conn:
                for line in source_conn.iterdump():
                    if self.canceled:
                        break
                    
                    dest_conn.execute(line)
                    self.progress.emit(50, "正在备份数据...")
            
            if not self.canceled:
                # 计算校验和
                self.progress.emit(80, "计算校验和...")
                checksum = self.calculate_checksum(self.dest_path)
                
                # 保存备份元数据
                backup_info = {
                    'source': self.source_path,
                    'timestamp': datetime.now().isoformat(),
                    'checksum': checksum,
                    'version': ProjectInfo.VERSION
                }
                
                with open(f"{self.dest_path}.meta", 'w') as f:
                    json.dump(backup_info, f)
                
                self.finished.emit(True, "备份完成")
            else:
                self.finished.emit(False, "备份已取消")
                
        except Exception as e:
            self.finished.emit(False, f"备份失败: {str(e)}")
        finally:
            if 'source_conn' in locals():
                source_conn.close()
            if 'dest_conn' in locals():
                dest_conn.close()

    def calculate_checksum(self, file_path):
        """计算文件校验和"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def cancel(self):
        """取消备份操作"""
        self.canceled = True


class DatabaseEncryptThread(QThread):
    """数据库加密线程"""
    progress = pyqtSignal(int, str)
    finished = pyqtSignal(bool, str)

    def __init__(self, db_path, password, encrypt=True, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.password = password
        self.encrypt = encrypt
        self.canceled = False

    def run(self):
        try:
            temp_path = f"{self.db_path}.tmp"
            
            if self.encrypt:
                self.progress.emit(10, "正在加密数据库...")
                # 加密逻辑
                # 这里只是示例，实际加密需要使用SQLCipher等库
                with open(self.db_path, 'rb') as f_in:
                    data = f_in.read()
                
                if self.canceled:
                    self.finished.emit(False, "加密已取消")
                    return
                
                # 简单的XOR加密（仅示例，生产环境应使用更强的加密）
                key = hashlib.sha256(self.password.encode()).digest()
                encrypted_data = bytes([data[i] ^ key[i % len(key)] for i in range(len(data))])
                
                with open(temp_path, 'wb') as f_out:
                    f_out.write(encrypted_data)
                
                os.replace(temp_path, self.db_path)
                self.finished.emit(True, "加密完成")
            else:
                self.progress.emit(10, "正在解密数据库...")
                # 解密逻辑
                with open(self.db_path, 'rb') as f_in:
                    encrypted_data = f_in.read()
                
                if self.canceled:
                    self.finished.emit(False, "解密已取消")
                    return
                
                # 简单的XOR解密（仅示例）
                key = hashlib.sha256(self.password.encode()).digest()
                data = bytes([encrypted_data[i] ^ key[i % len(key)] for i in range(len(encrypted_data))])
                
                with open(temp_path, 'wb') as f_out:
                    f_out.write(data)
                
                os.replace(temp_path, self.db_path)
                self.finished.emit(True, "解密完成")
                
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            self.finished.emit(False, f"{'加密' if self.encrypt else '解密'}失败: {str(e)}")

    def cancel(self):
        """取消加密/解密操作"""
        self.canceled = True


class SQLHighlighter(QSyntaxHighlighter):
    """SQL语法高亮"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.highlightingRules = []
        
        # SQL 关键字
        keywordFormat = QTextCharFormat()
        keywordFormat.setForeground(QColor(0, 0, 255))
        keywordFormat.setFontWeight(QFont.Bold)
        
        keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
            "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "VIEW", "TRIGGER",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",
            "NULL", "NOT", "AND", "OR", "AS", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
            "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "UNION", "ALL",
            "DISTINCT", "EXISTS", "BETWEEN", "LIKE", "IN", "IS", "ASC", "DESC", "BEGIN",
            "COMMIT", "ROLLBACK", "TRANSACTION", "EXPLAIN", "PRAGMA", "VACUUM", "ANALYZE",
            "ATTACH", "DETACH", "DATABASE", "WITH", "RECURSIVE", "CASE", "WHEN", "THEN",
            "ELSE", "END", "CAST", "COLUMN", "CONSTRAINT", "TEMPORARY", "TEMP", "IF",
            "EXISTS", "REPLACE", "RENAME", "TO", "ADD", "COLUMN", "WITHOUT", "ROWID",
            "USING", "NATURAL", "CROSS", "INTERSECT", "EXCEPT", "COLLATE", "GLOB",
            "REGEXP", "MATCH", "ESCAPE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
            "TRUE", "FALSE", "INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC", "BOOLEAN",
            "DATE", "DATETIME", "VARCHAR", "CHAR", "DECIMAL", "FLOAT", "DOUBLE", "INT",
            "BIGINT", "SMALLINT", "TINYINT", "MEDIUMINT", "VARBINARY", "TIMESTAMP"
        ]
        
        for word in keywords:
            pattern = QRegularExpression(f"\\b{word}\\b", QRegularExpression.CaseInsensitiveOption)
            self.highlightingRules.append((pattern, keywordFormat))
        
        # 字符串
        stringFormat = QTextCharFormat()
        stringFormat.setForeground(QColor(163, 21, 21))
        pattern = QRegularExpression("'[^']*'")
        self.highlightingRules.append((pattern, stringFormat))
        
        # 数字
        numberFormat = QTextCharFormat()
        numberFormat.setForeground(QColor(0, 128, 0))
        pattern = QRegularExpression("\\b[0-9]+\\b")
        self.highlightingRules.append((pattern, numberFormat))
        
        # 注释
        commentFormat = QTextCharFormat()
        commentFormat.setForeground(QColor(128, 128, 128))
        commentFormat.setFontItalic(True)
        pattern = QRegularExpression("--[^\n]*")
        self.highlightingRules.append((pattern, commentFormat))
        
        # 多行注释
        multiLineCommentFormat = QTextCharFormat()
        multiLineCommentFormat.setForeground(QColor(128, 128, 128))
        multiLineCommentFormat.setFontItalic(True)
        self.multiLineCommentFormat = multiLineCommentFormat
        
        # 多行注释开始和结束
        self.commentStartExpression = QRegularExpression("/\\*")
        self.commentEndExpression = QRegularExpression("\\*/")
    
    def highlightBlock(self, text):
        # 单行规则
        for pattern, format in self.highlightingRules:
            matchIterator = pattern.globalMatch(text)
            while matchIterator.hasNext():
                match = matchIterator.next()
                self.setFormat(match.capturedStart(), match.capturedLength(), format)
        
        # 多行注释处理
        self.setCurrentBlockState(0)
        
        startIndex = 0
        if self.previousBlockState() != 1:
            startIndex = text.indexOf(self.commentStartExpression)
        
        while startIndex >= 0:
            match = self.commentEndExpression.match(text, startIndex)
            endIndex = match.capturedStart()
            commentLength = 0
            
            if endIndex == -1:
                self.setCurrentBlockState(1)
                commentLength = len(text) - startIndex
            else:
                commentLength = endIndex - startIndex + match.capturedLength()
            
            self.setFormat(startIndex, commentLength, self.multiLineCommentFormat)
            startIndex = text.indexOf(self.commentStartExpression, startIndex + commentLength)


class SQLCompleter(QCompleter):
    """SQL自动完成"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setCaseSensitivity(Qt.CaseInsensitive)
        self.setCompletionMode(QCompleter.PopupCompletion)
        self.setModelSorting(QCompleter.CaseInsensitivelySortedModel)
        
        # 初始化关键词
        keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
            "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "VIEW", "TRIGGER",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",
            "NULL", "NOT", "AND", "OR", "AS", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
            "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "UNION", "ALL",
            "DISTINCT", "EXISTS", "BETWEEN", "LIKE", "IN", "IS", "ASC", "DESC", "BEGIN",
            "COMMIT", "ROLLBACK", "TRANSACTION", "EXPLAIN", "PRAGMA", "VACUUM", "ANALYZE",
            "ATTACH", "DETACH", "DATABASE", "WITH", "RECURSIVE", "CASE", "WHEN", "THEN",
            "ELSE", "END", "CAST", "COLUMN", "CONSTRAINT", "TEMPORARY", "TEMP", "IF",
            "EXISTS", "REPLACE", "RENAME", "TO", "ADD", "COLUMN", "WITHOUT", "ROWID",
            "USING", "NATURAL", "CROSS", "INTERSECT", "EXCEPT", "COLLATE", "GLOB",
            "REGEXP", "MATCH", "ESCAPE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP"
        ]
        
        model = QStringListModel()
        model.setStringList(keywords)
        self.setModel(model)


class DatabaseTab(QWidget):
    """数据库标签页"""
    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.db_path = db_path
        self.parent = parent
        self.init_ui()
    
    def init_ui(self):
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # 数据库信息
        info_layout = QHBoxLayout()
        
        self.db_name_label = QLabel(os.path.basename(self.db_path))
        self.db_name_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        
        self.db_stats_label = QLabel()
        self.db_stats_label.setStyleSheet("color: #666;")
        
        info_layout.addWidget(self.db_name_label)
        info_layout.addStretch()
        info_layout.addWidget(self.db_stats_label)
        
        layout.addLayout(info_layout)
        
        # 主选项卡
        self.tab_widget = QTabWidget()
        self.tab_widget.setTabsClosable(True)
        self.tab_widget.tabCloseRequested.connect(self.close_tab)
        layout.addWidget(self.tab_widget)
        
        # 初始加载表
        self.load_tables()
    
    def load_tables(self):
        """加载数据库表"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            
            # 为每个表创建标签页
            for table in tables:
                self.create_table_tab(table, conn)
            
            # 添加系统表标签页
            self.create_system_tables_tab(conn)
            
            # 更新统计信息
            self.update_stats(conn)
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"加载表失败:\n{str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()
    
    def create_table_tab(self, table_name, conn):
        """创建表数据标签页"""
        try:
            # 创建表数据视图
            table_widget = QWidget()
            layout = QVBoxLayout()
            table_widget.setLayout(layout)
            
            # 表信息
            info_widget = QWidget()
            info_layout = QHBoxLayout()
            info_widget.setLayout(info_layout)
            
            info_label = QLabel(f"表: {table_name}")
            info_label.setStyleSheet("font-weight: bold; color: #333;")
            info_layout.addWidget(info_label)
            
            # 添加索引按钮
            index_button = QPushButton("管理索引...")
            index_button.clicked.connect(lambda _, t=table_name: self.parent.manage_indexes(t, self.db_path))
            info_layout.addWidget(index_button)
            
            # 添加结构按钮
            structure_button = QPushButton("查看结构...")
            structure_button.clicked.connect(lambda _, t=table_name: self.show_table_structure(t, self.db_path))
            info_layout.addWidget(structure_button)
            
            info_layout.addStretch()
            
            layout.addWidget(info_widget)
            
            # 表数据视图
            table_view = QTableView()
            table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
            table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
            table_view.setContextMenuPolicy(Qt.CustomContextMenu)
            table_view.customContextMenuRequested.connect(
                lambda pos, view=table_view, t=table_name: self.parent.show_table_context_menu(pos, view, t, self.db_path))
            
            # 获取表数据
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1000")
            data = cursor.fetchall()
            
            # 创建模型
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(column_names)
            
            for row in data:
                items = [QStandardItem(str(item) if item is not None else "NULL") for item in row]
                model.appendRow(items)
            
            # 设置代理模型以支持排序
            proxy_model = QSortFilterProxyModel()
            proxy_model.setSourceModel(model)
            table_view.setModel(proxy_model)
            
            # 启用排序
            table_view.setSortingEnabled(True)
            
            # 调整列宽
            table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
            table_view.horizontalHeader().setStretchLastSection(True)
            
            # 按钮区域
            button_layout = QHBoxLayout()
            
            add_button = QPushButton(QIcon.fromTheme("list-add"), "添加记录")
            add_button.clicked.connect(lambda _, t=table_name: self.parent.add_record(t, self.db_path))
            button_layout.addWidget(add_button)
            
            edit_button = QPushButton(QIcon.fromTheme("document-edit"), "编辑记录")
            edit_button.clicked.connect(lambda _, t=table_name, v=table_view: self.parent.edit_record(t, v, self.db_path))
            button_layout.addWidget(edit_button)
            
            delete_button = QPushButton(QIcon.fromTheme("list-remove"), "删除记录")
            delete_button.clicked.connect(lambda _, t=table_name, v=table_view: self.parent.delete_records(t, v, self.db_path))
            button_layout.addWidget(delete_button)
            
            export_button = QPushButton(QIcon.fromTheme("document-save-as"), "导出数据...")
            export_button.clicked.connect(lambda _, t=table_name: self.parent.export_table_data(t, self.db_path))
            button_layout.addWidget(export_button)
            
            button_layout.addStretch()
            
            # 添加搜索框
            search_label = QLabel("搜索:")
            search_edit = QLineEdit()
            search_edit.setPlaceholderText("输入搜索内容...")
            search_edit.textChanged.connect(lambda text, pm=proxy_model: pm.setFilterFixedString(text))
            
            button_layout.addWidget(search_label)
            button_layout.addWidget(search_edit)
            
            layout.addWidget(table_view)
            layout.addLayout(button_layout)
            
            # 添加标签页
            self.tab_widget.addTab(table_widget, table_name)
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法加载表 {table_name}:\n{str(e)}")
    
    def create_system_tables_tab(self, conn):
        """创建系统表标签页"""
        try:
            # 创建系统表视图
            sys_tables_widget = QWidget()
            layout = QVBoxLayout()
            sys_tables_widget.setLayout(layout)
            
            # 系统表信息
            info_label = QLabel("系统表")
            info_label.setStyleSheet("font-weight: bold; color: #333;")
            layout.addWidget(info_label)
            
            # 系统表视图
            sys_tables_view = QTableView()
            
            # 获取系统表数据
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sqlite_master")
            data = cursor.fetchall()
            
            # 创建模型
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(["类型", "名称", "表名", "根页", "SQL"])
            
            for row in data:
                items = [QStandardItem(str(item)) for item in row]
                model.appendRow(items)
            
            # 设置代理模型以支持排序
            proxy_model = QSortFilterProxyModel()
            proxy_model.setSourceModel(model)
            sys_tables_view.setModel(proxy_model)
            
            # 启用排序
            sys_tables_view.setSortingEnabled(True)
            
            sys_tables_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
            sys_tables_view.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
            
            layout.addWidget(sys_tables_view)
            
            # 添加标签页
            self.tab_widget.addTab(sys_tables_widget, "系统表")
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法加载系统表:\n{str(e)}")
    
    def show_table_structure(self, table_name, db_path):
        """显示表结构"""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # 获取表结构信息
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            create_sql = cursor.fetchone()[0]
            
            # 获取索引信息
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            
            # 创建对话框
            dialog = QDialog(self)
            dialog.setWindowTitle(f"表结构 - {table_name}")
            dialog.resize(800, 600)
            
            layout = QVBoxLayout()
            dialog.setLayout(layout)
            
            # 选项卡
            tab_widget = QTabWidget()
            
            # 列信息
            columns_widget = QWidget()
            columns_layout = QVBoxLayout()
            columns_widget.setLayout(columns_layout)
            
            # 列表格
            columns_table = QTableView()
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(["CID", "名称", "类型", "非空", "默认值", "主键"])
            
            for col in columns:
                cid = QStandardItem(str(col[0]))
                name = QStandardItem(col[1])
                type_ = QStandardItem(col[2])
                notnull = QStandardItem("是" if col[3] else "否")
                dflt_value = QStandardItem(str(col[4]) if col[4] is not None else "")
                pk = QStandardItem(str(col[5]))
                
                model.appendRow([cid, name, type_, notnull, dflt_value, pk])
            
            columns_table.setModel(model)
            columns_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            columns_layout.addWidget(columns_table)
            tab_widget.addTab(columns_widget, "列")
            
            # SQL定义
            sql_widget = QWidget()
            sql_layout = QVBoxLayout()
            sql_widget.setLayout(sql_layout)
            
            sql_edit = QTextEdit()
            sql_edit.setPlainText(create_sql)
            sql_edit.setReadOnly(True)
            sql_edit.setFont(QFont("Consolas", 10))
            
            sql_layout.addWidget(sql_edit)
            tab_widget.addTab(sql_widget, "SQL定义")
            
            # 索引信息
            if indexes:
                indexes_widget = QWidget()
                indexes_layout = QVBoxLayout()
                indexes_widget.setLayout(indexes_layout)
                
                indexes_table = QTableView()
                model = QStandardItemModel()
                model.setHorizontalHeaderLabels(["序号", "名称", "唯一", "SQL"])
                
                for idx in indexes:
                    seq = QStandardItem(str(idx[0]))
                    name = QStandardItem(idx[1])
                    unique = QStandardItem("是" if idx[2] else "否")
                    
                    # 获取索引SQL
                    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='index' AND name='{idx[1]}'")
                    result = cursor.fetchone()
                    sql = result[0] if result else ""
                    
                    sql_item = QStandardItem(sql)
                    
                    model.appendRow([seq, name, unique, sql_item])
                
                indexes_table.setModel(model)
                indexes_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
                
                indexes_layout.addWidget(indexes_table)
                tab_widget.addTab(indexes_widget, "索引")
            
            layout.addWidget(tab_widget)
            
            # 按钮
            button_box = QDialogButtonBox(QDialogButtonBox.Ok)
            button_box.accepted.connect(dialog.accept)
            layout.addWidget(button_box)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法获取表结构:\n{str(e)}")
        finally:
            if 'conn' in locals():
                conn.close()
    
    def update_stats(self, conn):
        """更新数据库统计信息"""
        try:
            cursor = conn.cursor()
            
            # 获取表数量
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            
            # 获取总行数
            total_rows = 0
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    total_rows += cursor.fetchone()[0]
                except:
                    continue
            
            # 获取数据库大小
            db_size = os.path.getsize(self.db_path) if self.db_path else 0
            size_str = self.parent.format_file_size(db_size)
            
            self.db_stats_label.setText(f"表: {table_count} | 总行数: {total_rows} | 大小: {size_str}")
            
        except Exception as e:
            self.db_stats_label.setText("统计信息不可用")
    
    def close_tab(self, index):
        """关闭标签页"""
        self.tab_widget.removeTab(index)


class DatabaseManager(QMainWindow):
    """主窗口类"""
    database_opened = pyqtSignal(str)
    database_closed = pyqtSignal()
    
    def __init__(self, db_path=None):
        super().__init__()
        self.setWindowTitle(f"{ProjectInfo.NAME} {ProjectInfo.VERSION} (Build: {ProjectInfo.BUILD_DATE})")
        self.setWindowIcon(QIcon("icon.ico"))
        self.resize(1400, 900)
        
        # 初始化数据库连接
        self.current_db_path = None
        self.open_databases = {}  # {db_path: conn}
        
        # 初始化UI
        self.init_ui()
        
        # 加载设置
        self.settings = QSettings("SQLiteManagerPro", "DatabaseManager")
        self.load_settings()
        
        # 初始化系统托盘
        self.init_system_tray()
        
        # 如果提供了数据库路径，直接打开
        if db_path:
            QTimer.singleShot(100, lambda: self.open_database(db_path))
    
    def init_ui(self):
        # 主窗口部件
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        # 主布局
        main_layout = QVBoxLayout()
        main_widget.setLayout(main_layout)
        
        # 创建菜单栏
        self.create_menu_bar()
        
        # 创建工具栏
        self.create_toolbar()
        
        # 主内容区域
        self.content_splitter = QSplitter(Qt.Vertical)
        main_layout.addWidget(self.content_splitter)
        
        # 数据库选项卡
        self.db_tab_widget = QTabWidget()
        self.db_tab_widget.setTabsClosable(True)
        self.db_tab_widget.tabCloseRequested.connect(self.close_database_tab)
        self.db_tab_widget.currentChanged.connect(self.on_db_tab_changed)
        
        # 数据库信息区域
        self.db_info_widget = QWidget()
        db_info_layout = QHBoxLayout()
        self.db_info_widget.setLayout(db_info_layout)
        
        self.db_path_label = QLabel("未打开数据库")
        self.db_path_label.setStyleSheet("font-weight: bold; color: #333;")
        
        self.db_stats_label = QLabel()
        self.db_stats_label.setStyleSheet("color: #666;")
        
        db_info_layout.addWidget(self.db_path_label)
        db_info_layout.addStretch()
        db_info_layout.addWidget(self.db_stats_label)
        
        self.content_splitter.addWidget(self.db_info_widget)
        self.content_splitter.addWidget(self.db_tab_widget)
        
        # SQL查询区域
        self.sql_widget = QWidget()
        sql_layout = QVBoxLayout()
        self.sql_widget.setLayout(sql_layout)
        
        # SQL编辑器工具栏
        sql_toolbar = QToolBar()
        sql_toolbar.setObjectName("sqlToolBar")
        sql_toolbar.setIconSize(QSize(16, 16))
        
        self.execute_action = QAction(QIcon.fromTheme("media-playback-start"), "执行 (F5)", self)
        self.execute_action.setShortcut(QKeySequence("F5"))
        self.execute_action.triggered.connect(self.execute_sql)
        sql_toolbar.addAction(self.execute_action)
        
        self.explain_action = QAction(QIcon.fromTheme("system-run"), "解释执行计划 (F6)", self)
        self.explain_action.setShortcut(QKeySequence("F6"))
        self.explain_action.triggered.connect(self.explain_sql)
        sql_toolbar.addAction(self.explain_action)
        
        self.clear_action = QAction(QIcon.fromTheme("edit-clear"), "清除", self)
        self.clear_action.triggered.connect(self.clear_sql)
        sql_toolbar.addAction(self.clear_action)
        
        self.format_action = QAction(QIcon.fromTheme("format-indent-more"), "格式化SQL", self)
        self.format_action.triggered.connect(self.format_sql)
        sql_toolbar.addAction(self.format_action)
        
        self.history_action = QAction(QIcon.fromTheme("document-open-recent"), "历史记录", self)
        self.history_action.triggered.connect(self.show_sql_history)
        sql_toolbar.addAction(self.history_action)
        
        sql_layout.addWidget(sql_toolbar)
        
        # SQL编辑器
        self.sql_editor = QTextEdit()
        self.sql_editor.setPlaceholderText("输入SQL查询语句...")
        self.sql_editor.setFont(QFont("Consolas", 10))
        self.highlighter = SQLHighlighter(self.sql_editor.document())
        
        # 设置自动完成
        self.completer = SQLCompleter(self)
        self.completer.setWidget(self.sql_editor)
        self.sql_editor.textChanged.connect(self.update_completer)
        
        sql_layout.addWidget(self.sql_editor)
        
        # SQL结果区域
        self.sql_result_tab = QTabWidget()
        
        # 结果表格
        self.sql_result_table = QTableView()
        self.sql_result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sql_result_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.sql_result_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.sql_result_table.customContextMenuRequested.connect(self.show_sql_result_context_menu)
        
        # 结果文本
        self.sql_result_text = QTextEdit()
        self.sql_result_text.setReadOnly(True)
        self.sql_result_text.setFont(QFont("Consolas", 10))
        
        # 可视化结果
        self.visualization_view = QGraphicsView()
        self.visualization_scene = QGraphicsScene()
        self.visualization_view.setScene(self.visualization_scene)
        
        self.sql_result_tab.addTab(self.sql_result_table, "表格视图")
        self.sql_result_tab.addTab(self.sql_result_text, "文本视图")
        self.sql_result_tab.addTab(self.visualization_view, "可视化")
        
        sql_layout.addWidget(self.sql_result_tab)
        
        self.content_splitter.addWidget(self.sql_widget)
        
        # 设置分割器比例
        self.content_splitter.setSizes([50, 500, 350])
        
        # 状态栏
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        
        # 连接信号
        self.database_opened.connect(self.on_database_opened)
        self.database_closed.connect(self.on_database_closed)
    
    def create_menu_bar(self):
        menubar = self.menuBar()
        menubar.setObjectName("mainMenuBar")
        
        # 文件菜单
        file_menu = menubar.addMenu("文件")
        
        self.open_action = QAction(QIcon.fromTheme("document-open"), "打开数据库...", self)
        self.open_action.setShortcut(QKeySequence.Open)
        self.open_action.triggered.connect(self.open_database_dialog)
        file_menu.addAction(self.open_action)
        
        self.close_action = QAction(QIcon.fromTheme("window-close"), "关闭数据库", self)
        self.close_action.setEnabled(False)
        self.close_action.triggered.connect(self.close_current_database)
        file_menu.addAction(self.close_action)
        
        file_menu.addSeparator()
        
        self.backup_action = QAction(QIcon.fromTheme("document-save"), "备份数据库...", self)
        self.backup_action.setEnabled(False)
        self.backup_action.triggered.connect(self.backup_database_dialog)
        file_menu.addAction(self.backup_action)
        
        self.restore_action = QAction(QIcon.fromTheme("document-revert"), "恢复数据库...", self)
        self.restore_action.setEnabled(False)
        self.restore_action.triggered.connect(self.restore_database_dialog)
        file_menu.addAction(self.restore_action)
        
        file_menu.addSeparator()
        
        self.export_action = QAction(QIcon.fromTheme("document-export"), "导出数据...", self)
        self.export_action.setEnabled(False)
        self.export_action.triggered.connect(self.export_data_dialog)
        file_menu.addAction(self.export_action)
        
        self.import_action = QAction(QIcon.fromTheme("document-import"), "导入数据...", self)
        self.import_action.setEnabled(False)
        self.import_action.triggered.connect(self.import_data_dialog)
        file_menu.addAction(self.import_action)
        
        file_menu.addSeparator()
        
        self.recent_menu = file_menu.addMenu(QIcon.fromTheme("document-open-recent"), "最近文件")
        
        file_menu.addSeparator()
        
        exit_action = QAction(QIcon.fromTheme("application-exit"), "退出", self)
        exit_action.setShortcut(QKeySequence.Quit)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # 编辑菜单
        edit_menu = menubar.addMenu("编辑")
        
        self.copy_action = QAction(QIcon.fromTheme("edit-copy"), "复制", self)
        self.copy_action.setShortcut(QKeySequence.Copy)
        self.copy_action.triggered.connect(self.copy_selected_content)
        edit_menu.addAction(self.copy_action)
        
        self.paste_action = QAction(QIcon.fromTheme("edit-paste"), "粘贴", self)
        self.paste_action.setShortcut(QKeySequence.Paste)
        self.paste_action.triggered.connect(self.paste_content)
        edit_menu.addAction(self.paste_action)
        
        edit_menu.addSeparator()
        
        self.history_menu = edit_menu.addMenu(QIcon.fromTheme("document-open-recent"), "SQL历史记录")
        
        # 视图菜单
        view_menu = menubar.addMenu("视图")
        
        self.refresh_action = QAction(QIcon.fromTheme("view-refresh"), "刷新", self)
        self.refresh_action.setShortcut(QKeySequence.Refresh)
        self.refresh_action.setEnabled(False)
        self.refresh_action.triggered.connect(self.refresh_current_database)
        view_menu.addAction(self.refresh_action)
        
        view_menu.addSeparator()
        
        self.dark_theme_action = QAction("深色主题", self)
        self.dark_theme_action.setCheckable(True)
        self.dark_theme_action.triggered.connect(self.toggle_theme)
        view_menu.addAction(self.dark_theme_action)
        
        self.compact_layout_action = QAction("紧凑布局", self)
        self.compact_layout_action.setCheckable(True)
        self.compact_layout_action.triggered.connect(self.toggle_layout)
        view_menu.addAction(self.compact_layout_action)
        
        # 工具菜单
        tools_menu = menubar.addMenu("工具")
        
        self.create_table_action = QAction(QIcon.fromTheme("document-new"), "创建表...", self)
        self.create_table_action.setEnabled(False)
        self.create_table_action.triggered.connect(self.create_table_dialog)
        tools_menu.addAction(self.create_table_action)
        
        self.drop_table_action = QAction(QIcon.fromTheme("edit-delete"), "删除表...", self)
        self.drop_table_action.setEnabled(False)
        self.drop_table_action.triggered.connect(self.drop_table_dialog)
        tools_menu.addAction(self.drop_table_action)
        
        tools_menu.addSeparator()
        
        self.optimize_action = QAction(QIcon.fromTheme("tools-wizard"), "优化数据库", self)
        self.optimize_action.setEnabled(False)
        self.optimize_action.triggered.connect(self.optimize_database)
        tools_menu.addAction(self.optimize_action)
        
        self.integrity_check_action = QAction(QIcon.fromTheme("tools-check-spelling"), "完整性检查", self)
        self.integrity_check_action.setEnabled(False)
        self.integrity_check_action.triggered.connect(self.check_database_integrity)
        tools_menu.addAction(self.integrity_check_action)
        
        tools_menu.addSeparator()
        
        self.encrypt_action = QAction(QIcon.fromTheme("document-encrypt"), "加密数据库...", self)
        self.encrypt_action.setEnabled(False)
        self.encrypt_action.triggered.connect(self.encrypt_database_dialog)
        tools_menu.addAction(self.encrypt_action)
        
        self.decrypt_action = QAction(QIcon.fromTheme("document-decrypt"), "解密数据库...", self)
        self.decrypt_action.setEnabled(False)
        self.decrypt_action.triggered.connect(self.decrypt_database_dialog)
        tools_menu.addAction(self.decrypt_action)
        
        # 帮助菜单
        help_menu = menubar.addMenu("帮助")
        
        help_action = QAction(QIcon.fromTheme("help-contents"), "帮助", self)
        help_action.triggered.connect(self.show_help)
        help_menu.addAction(help_action)
        
        about_action = QAction(QIcon.fromTheme("help-about"), "关于", self)
        about_action.triggered.connect(self.show_about_dialog)
        help_menu.addAction(about_action)
        
        check_updates_action = QAction(QIcon.fromTheme("system-software-update"), "检查更新", self)
        check_updates_action.triggered.connect(self.check_for_updates)
        help_menu.addAction(check_updates_action)
    
    def create_toolbar(self):
        toolbar = self.addToolBar("工具栏")
        toolbar.setObjectName("mainToolBar")
        toolbar.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)
        toolbar.setIconSize(QSize(24, 24))
        
        toolbar.addAction(self.open_action)
        toolbar.addAction(self.close_action)
        toolbar.addSeparator()
        toolbar.addAction(self.backup_action)
        toolbar.addAction(self.restore_action)
        toolbar.addSeparator()
        toolbar.addAction(self.refresh_action)
        toolbar.addSeparator()
        toolbar.addAction(self.create_table_action)
        toolbar.addAction(self.drop_table_action)
        toolbar.addSeparator()
        toolbar.addAction(self.export_action)
        toolbar.addAction(self.import_action)
        toolbar.addSeparator()
        toolbar.addAction(self.optimize_action)
    
    def init_system_tray(self):
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(QIcon("icon.ico"))
        
        tray_menu = QMenu()
        show_action = tray_menu.addAction("显示")
        show_action.triggered.connect(self.showNormal)
        
        exit_action = tray_menu.addAction("退出")
        exit_action.triggered.connect(self.close)
        
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()
        
        self.tray_icon.activated.connect(self.tray_icon_activated)
    
    def tray_icon_activated(self, reason):
        if reason == QSystemTrayIcon.DoubleClick:
            self.showNormal()
    
    def load_settings(self):
        # 恢复窗口大小和位置
        geometry = self.settings.value("windowGeometry")
        if geometry:
            self.restoreGeometry(geometry)
        
        state = self.settings.value("windowState")
        if state:
            self.restoreState(state)
        
        # 加载主题设置
        dark_theme = self.settings.value("darkTheme", False, type=bool)
        self.dark_theme_action.setChecked(dark_theme)
        self.toggle_theme(dark_theme)
        
        # 加载布局设置
        compact_layout = self.settings.value("compactLayout", False, type=bool)
        self.compact_layout_action.setChecked(compact_layout)
        self.toggle_layout(compact_layout)
        
        # 加载最近文件
        recent_files = self.settings.value("recentFiles", [])
        self.update_recent_files_menu(recent_files)
        
        # 加载SQL历史记录
        sql_history = self.settings.value("sqlHistory", [])
        self.update_sql_history_menu(sql_history)
    
    def save_settings(self):
        self.settings.setValue("windowGeometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
        self.settings.setValue("darkTheme", self.dark_theme_action.isChecked())
        self.settings.setValue("compactLayout", self.compact_layout_action.isChecked())
        
        # 保存SQL历史记录
        if hasattr(self, 'sql_history'):
            self.settings.setValue("sqlHistory", self.sql_history)
    
    def update_recent_files_menu(self, recent_files):
        self.recent_menu.clear()
        
        if not recent_files:
            self.recent_menu.setEnabled(False)
            return
        
        self.recent_menu.setEnabled(True)
        
        for i, file_path in enumerate(recent_files[:10]):
            action = QAction(f"{i+1}. {QFileInfo(file_path).fileName()}", self)
            action.setData(file_path)
            action.triggered.connect(self.open_recent_file)
            self.recent_menu.addAction(action)
        
        self.recent_menu.addSeparator()
        clear_action = QAction("清除列表", self)
        clear_action.triggered.connect(self.clear_recent_files)
        self.recent_menu.addAction(clear_action)
    
    def update_sql_history_menu(self, history):
        self.history_menu.clear()
        
        if not hasattr(self, 'sql_history'):
            self.sql_history = history if history else []
        
        if not self.sql_history:
            self.history_menu.setEnabled(False)
            return
        
        self.history_menu.setEnabled(True)
        
        for i, sql in enumerate(reversed(self.sql_history[:20])):
            # 截断长SQL语句
            display_text = sql[:50] + "..." if len(sql) > 50 else sql
            action = QAction(f"{len(self.sql_history)-i}. {display_text}", self)
            action.setData(sql)
            action.triggered.connect(lambda _, s=sql: self.load_sql_from_history(s))
            self.history_menu.addAction(action)
        
        self.history_menu.addSeparator()
        clear_action = QAction("清除历史记录", self)
        clear_action.triggered.connect(self.clear_sql_history)
        self.history_menu.addAction(clear_action)
    
    def add_to_sql_history(self, sql):
        if not hasattr(self, 'sql_history'):
            self.sql_history = []
        
        # 去除前后空格和空行
        sql = sql.strip()
        if not sql:
            return
        
        # 如果已经存在相同的SQL，先移除
        if sql in self.sql_history:
            self.sql_history.remove(sql)
        
        # 添加到历史记录
        self.sql_history.append(sql)
        
        # 限制历史记录数量
        if len(self.sql_history) > 100:
            self.sql_history = self.sql_history[-100:]
        
        # 更新菜单
        self.update_sql_history_menu(self.sql_history)
    
    def load_sql_from_history(self, sql):
        self.sql_editor.setPlainText(sql)
        self.sql_editor.setFocus()
    
    def clear_sql_history(self):
        self.sql_history = []
        self.update_sql_history_menu(self.sql_history)
        self.settings.remove("sqlHistory")
    
    def open_recent_file(self):
        action = self.sender()
        if action:
            file_path = action.data()
            if os.path.exists(file_path):
                self.open_database(file_path)
            else:
                QMessageBox.warning(self, "错误", "文件不存在或已被移动")
                self.remove_recent_file(file_path)
    
    def clear_recent_files(self):
        self.settings.setValue("recentFiles", [])
        self.recent_menu.clear()
        self.recent_menu.setEnabled(False)
    
    def remove_recent_file(self, file_path):
        recent_files = self.settings.value("recentFiles", [])
        if file_path in recent_files:
            recent_files.remove(file_path)
            self.settings.setValue("recentFiles", recent_files)
            self.update_recent_files_menu(recent_files)
    
    def add_to_recent_files(self, file_path):
        recent_files = self.settings.value("recentFiles", [])
        
        if file_path in recent_files:
            recent_files.remove(file_path)
        
        recent_files.insert(0, file_path)
        recent_files = recent_files[:10]  # 保留最近10个文件
        
        self.settings.setValue("recentFiles", recent_files)
        self.update_recent_files_menu(recent_files)
    
    def on_database_opened(self, db_path):
        self.close_action.setEnabled(True)
        self.refresh_action.setEnabled(True)
        self.create_table_action.setEnabled(True)
        self.drop_table_action.setEnabled(True)
        self.export_action.setEnabled(True)
        self.import_action.setEnabled(True)
        self.backup_action.setEnabled(True)
        self.restore_action.setEnabled(True)
        self.optimize_action.setEnabled(True)
        self.integrity_check_action.setEnabled(True)
        self.encrypt_action.setEnabled(True)
        self.decrypt_action.setEnabled(True)
        
        self.db_path_label.setText(f"数据库: {db_path}")
        self.update_database_stats(db_path)
    
    def on_database_closed(self):
        if not self.open_databases:
            self.close_action.setEnabled(False)
            self.refresh_action.setEnabled(False)
            self.create_table_action.setEnabled(False)
            self.drop_table_action.setEnabled(False)
            self.export_action.setEnabled(False)
            self.import_action.setEnabled(False)
            self.backup_action.setEnabled(False)
            self.restore_action.setEnabled(False)
            self.optimize_action.setEnabled(False)
            self.integrity_check_action.setEnabled(False)
            self.encrypt_action.setEnabled(False)
            self.decrypt_action.setEnabled(False)
            
            self.db_path_label.setText("未打开数据库")
            self.db_stats_label.clear()
    
    def update_database_stats(self, db_path):
        if db_path not in self.open_databases:
            return
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表数量
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]
            
            # 获取总行数
            total_rows = 0
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                    total_rows += cursor.fetchone()[0]
                except:
                    continue
            
            # 获取数据库大小
            db_size = os.path.getsize(db_path) if db_path else 0
            size_str = self.format_file_size(db_size)
            
            self.db_stats_label.setText(f"表: {table_count} | 总行数: {total_rows} | 大小: {size_str}")
            
        except Exception as e:
            self.db_stats_label.setText("统计信息不可用")
    
    def format_file_size(self, size):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    
    def open_database_dialog(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "打开SQLite数据库", 
            self.settings.value("lastDir", ""),
            "SQLite数据库 (*.db *.sqlite *.sqlite3 *.db3);;所有文件 (*)"
        )
        
        if file_path:
            self.settings.setValue("lastDir", os.path.dirname(file_path))
            self.open_database(file_path)
    
    def open_database(self, db_path):
        try:
            # 检查是否已经打开
            if db_path in self.open_databases:
                # 切换到已打开的数据库
                index = next(i for i in range(self.db_tab_widget.count()) 
                           if self.db_tab_widget.widget(i).db_path == db_path)
                self.db_tab_widget.setCurrentIndex(index)
                return
            
            # 打开新数据库
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            self.open_databases[db_path] = conn
            
            # 添加事务支持
            conn.isolation_level = None
            
            # 创建数据库标签页
            db_tab = DatabaseTab(db_path, self)
            tab_index = self.db_tab_widget.addTab(db_tab, os.path.basename(db_path))
            self.db_tab_widget.setCurrentIndex(tab_index)
            
            # 更新UI
            self.database_opened.emit(db_path)
            
            # 添加到最近文件
            self.add_to_recent_files(db_path)
            
            self.status_bar.showMessage(f"成功打开数据库: {db_path}")
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法打开数据库:\n{str(e)}")
            self.status_bar.showMessage(f"打开数据库失败: {db_path}")
    
    def close_database_tab(self, index):
        """关闭数据库标签页"""
        db_tab = self.db_tab_widget.widget(index)
        db_path = db_tab.db_path
        
        # 关闭数据库连接
        if db_path in self.open_databases:
            self.open_databases[db_path].close()
            del self.open_databases[db_path]
        
        # 移除标签页
        self.db_tab_widget.removeTab(index)
        
        # 更新UI状态
        if not self.open_databases:
            self.database_closed.emit()
        
        self.status_bar.showMessage(f"已关闭数据库: {db_path}")
    
    def close_current_database(self):
        """关闭当前数据库"""
        current_index = self.db_tab_widget.currentIndex()
        if current_index >= 0:
            self.close_database_tab(current_index)
    
    def refresh_current_database(self):
        """刷新当前数据库"""
        current_index = self.db_tab_widget.currentIndex()
        if current_index >= 0:
            db_tab = self.db_tab_widget.widget(current_index)
            db_tab.load_tables()
            self.status_bar.showMessage("数据库已刷新")
    
    def on_db_tab_changed(self, index):
        """数据库标签页切换事件"""
        if index >= 0:
            db_tab = self.db_tab_widget.widget(index)
            self.db_path_label.setText(f"数据库: {db_tab.db_path}")
            self.update_database_stats(db_tab.db_path)
            self.current_db_path = db_tab.db_path
        else:
            self.db_path_label.setText("未打开数据库")
            self.db_stats_label.clear()
            self.current_db_path = None
    
    def backup_database_dialog(self):
        """数据库备份对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self, "备份数据库", 
            f"{os.path.basename(self.current_db_path)}.backup",
            "SQLite数据库 (*.db *.sqlite *.sqlite3 *.db3);;所有文件 (*)", 
            options=options
        )
        
        if file_path:
            # 创建备份线程
            self.backup_thread = DatabaseBackupThread(self.current_db_path, file_path)
            
            # 创建进度对话框
            progress = QProgressDialog("正在备份数据库...", "取消", 0, 100, self)
            progress.setWindowTitle("数据库备份")
            progress.setWindowModality(Qt.WindowModal)
            progress.setAutoClose(True)
            
            # 连接信号
            self.backup_thread.progress.connect(progress.setValue)
            self.backup_thread.progress.connect(lambda v, m: progress.setLabelText(m))
            self.backup_thread.finished.connect(
                lambda success, msg: QMessageBox.information(self, "完成", msg) if success else QMessageBox.critical(self, "错误", msg))
            self.backup_thread.finished.connect(progress.close)
            progress.canceled.connect(self.backup_thread.cancel)
            
            # 开始备份
            self.backup_thread.start()
    
    def restore_database_dialog(self):
        """数据库恢复对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        reply = QMessageBox.question(
            self, "确认", 
            "恢复操作将覆盖当前数据库，确定要继续吗?", 
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply != QMessageBox.Yes:
            return
        
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择备份文件", 
            self.settings.value("lastBackupDir", ""),
            "SQLite数据库 (*.db *.sqlite *.sqlite3 *.db3);;所有文件 (*)", 
            options=options
        )
        
        if file_path:
            self.settings.setValue("lastBackupDir", os.path.dirname(file_path))
            
            try:
                # 关闭当前数据库
                if self.current_db_path in self.open_databases:
                    self.open_databases[self.current_db_path].close()
                    del self.open_databases[self.current_db_path]
                
                # 复制备份文件
                import shutil
                shutil.copyfile(file_path, self.current_db_path)
                
                # 重新打开数据库
                self.open_database(self.current_db_path)
                
                QMessageBox.information(self, "成功", "数据库恢复成功")
                
            except Exception as e:
                QMessageBox.critical(self, "错误", f"恢复失败:\n{str(e)}")
    
    def optimize_database(self):
        """优化数据库"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        try:
            conn = self.open_databases[self.current_db_path]
            
            # 创建进度对话框
            progress = QProgressDialog("正在优化数据库...", None, 0, 100, self)
            progress.setWindowTitle("数据库优化")
            progress.setWindowModality(Qt.WindowModal)
            progress.setAutoClose(True)
            
            # 执行优化命令
            conn.execute("VACUUM")
            conn.execute("ANALYZE")
            conn.commit()
            
            progress.close()
            
            # 刷新数据库
            self.refresh_current_database()
            
            QMessageBox.information(self, "成功", "数据库优化完成")
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"优化失败:\n{str(e)}")
    
    def check_database_integrity(self):
        """检查数据库完整性"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        try:
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            
            # 执行完整性检查
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchall()
            
            # 显示结果
            dialog = QDialog(self)
            dialog.setWindowTitle("数据库完整性检查")
            dialog.resize(500, 300)
            
            layout = QVBoxLayout()
            dialog.setLayout(layout)
            
            text_edit = QTextEdit()
            text_edit.setReadOnly(True)
            text_edit.setFont(QFont("Consolas", 10))
            
            if len(result) == 1 and result[0][0] == "ok":
                text_edit.setPlainText("完整性检查通过: 数据库没有损坏")
            else:
                text_edit.setPlainText("\n".join(row[0] for row in result))
            
            layout.addWidget(text_edit)
            
            button_box = QDialogButtonBox(QDialogButtonBox.Ok)
            button_box.accepted.connect(dialog.accept)
            layout.addWidget(button_box)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"完整性检查失败:\n{str(e)}")
    
    def encrypt_database_dialog(self):
        """数据库加密对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        password, ok = QInputDialog.getText(
            self, "加密数据库", "输入加密密码:", 
            QLineEdit.Password
        )
        
        if ok and password:
            # 创建加密线程
            self.encrypt_thread = DatabaseEncryptThread(self.current_db_path, password, True)
            
            # 创建进度对话框
            progress = QProgressDialog("正在加密数据库...", "取消", 0, 100, self)
            progress.setWindowTitle("数据库加密")
            progress.setWindowModality(Qt.WindowModal)
            progress.setAutoClose(True)
            
            # 连接信号
            self.encrypt_thread.progress.connect(progress.setValue)
            self.encrypt_thread.progress.connect(lambda v, m: progress.setLabelText(m))
            self.encrypt_thread.finished.connect(
                lambda success, msg: QMessageBox.information(self, "完成", msg) if success else QMessageBox.critical(self, "错误", msg))
            self.encrypt_thread.finished.connect(progress.close)
            progress.canceled.connect(self.encrypt_thread.cancel)
            
            # 开始加密
            self.encrypt_thread.start()
    
    def decrypt_database_dialog(self):
        """数据库解密对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        password, ok = QInputDialog.getText(
            self, "解密数据库", "输入解密密码:", 
            QLineEdit.Password
        )
        
        if ok and password:
            # 创建解密线程
            self.decrypt_thread = DatabaseEncryptThread(self.current_db_path, password, False)
            
            # 创建进度对话框
            progress = QProgressDialog("正在解密数据库...", "取消", 0, 100, self)
            progress.setWindowTitle("数据库解密")
            progress.setWindowModality(Qt.WindowModal)
            progress.setAutoClose(True)
            
            # 连接信号
            self.decrypt_thread.progress.connect(progress.setValue)
            self.decrypt_thread.progress.connect(lambda v, m: progress.setLabelText(m))
            self.decrypt_thread.finished.connect(
                lambda success, msg: QMessageBox.information(self, "完成", msg) if success else QMessageBox.critical(self, "错误", msg))
            self.decrypt_thread.finished.connect(progress.close)
            progress.canceled.connect(self.decrypt_thread.cancel)
            
            # 开始解密
            self.decrypt_thread.start()
    
    def execute_sql(self):
        """执行SQL语句"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            QMessageBox.warning(self, "警告", "请输入SQL语句")
            return
        
        # 添加到历史记录
        self.add_to_sql_history(sql)
        
        try:
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            
            # 开始事务
            cursor.execute("BEGIN")
            
            cursor.execute(sql)
            
            if sql.lower().startswith("select"):
                # 查询结果显示
                data = cursor.fetchall()
                column_names = [description[0] for description in cursor.description]
                
                model = QStandardItemModel()
                model.setHorizontalHeaderLabels(column_names)
                
                for row in data:
                    items = [QStandardItem(str(item) if item is not None else "NULL") for item in row]
                    model.appendRow(items)
                
                # 设置代理模型以支持排序
                proxy_model = QSortFilterProxyModel()
                proxy_model.setSourceModel(model)
                self.sql_result_table.setModel(proxy_model)
                
                # 启用排序
                self.sql_result_table.setSortingEnabled(True)
                
                self.sql_result_tab.setCurrentIndex(0)
                self.sql_result_text.clear()
                
                # 尝试可视化数据
                self.visualize_data(data, column_names)
                
                self.status_bar.showMessage(f"查询成功，返回 {len(data)} 行")
            else:
                # 非查询操作
                conn.commit()
                
                # 刷新当前数据库
                current_index = self.db_tab_widget.currentIndex()
                if current_index >= 0:
                    self.db_tab_widget.widget(current_index).load_tables()
                
                affected_rows = cursor.rowcount if cursor.rowcount != -1 else "未知"
                self.sql_result_text.setPlainText(f"执行成功，影响 {affected_rows} 行")
                self.sql_result_tab.setCurrentIndex(1)
                
                self.status_bar.showMessage(f"执行成功，影响 {affected_rows} 行")
            
        except Exception as e:
            conn.rollback()
            error_msg = f"SQL执行失败:\n{str(e)}"
            QMessageBox.critical(self, "错误", error_msg)
            self.sql_result_text.setPlainText(error_msg)
            self.sql_result_tab.setCurrentIndex(1)
            self.status_bar.showMessage("SQL执行失败")
    
    def visualize_data(self, data, column_names):
        """可视化数据"""
        if not data or not column_names:
            return
        
        self.visualization_scene.clear()
        
        try:
            # 尝试确定数值列
            numeric_columns = []
            for i, col in enumerate(column_names):
                try:
                    float(data[0][i])
                    numeric_columns.append(i)
                except (ValueError, TypeError, IndexError):
                    continue
            
            if not numeric_columns:
                return
            
            # 简单柱状图
            scene_width = 800
            scene_height = 500
            margin = 50
            chart_width = scene_width - 2 * margin
            chart_height = scene_height - 2 * margin
            
            # 绘制坐标轴
            self.visualization_scene.addLine(margin, scene_height - margin, 
                                          scene_width - margin, scene_height - margin)
            self.visualization_scene.addLine(margin, scene_height - margin, 
                                          margin, margin)
            
            # 绘制数据
            max_value = max(float(row[numeric_columns[0]]) for row in data)
            bar_width = chart_width / len(data)
            
            for i, row in enumerate(data[:20]):  # 限制显示数量
                value = float(row[numeric_columns[0]])
                bar_height = (value / max_value) * chart_height if max_value > 0 else 0
                
                x = margin + i * bar_width
                y = scene_height - margin - bar_height
                
                # 绘制柱状
                bar = QGraphicsRectItem(x, y, bar_width * 0.8, bar_height)
                bar.setBrush(QBrush(QColor(70, 130, 180)))
                bar.setPen(QPen(Qt.black, 1))
                self.visualization_scene.addItem(bar)
                
                # 添加标签
                if i % 2 == 0 or len(data) <= 10:  # 限制标签数量
                    label = QGraphicsTextItem(str(value))
                    label.setPos(x, y - 20)
                    self.visualization_scene.addItem(label)
                    
                    # X轴标签
                    if len(column_names) > 1:
                        x_label = QGraphicsTextItem(str(row[1])[:10])  # 截断长文本
                        x_label.setPos(x, scene_height - margin + 5)
                        self.visualization_scene.addItem(x_label)
            
            # 添加标题
            title = QGraphicsTextItem(f"{column_names[numeric_columns[0]]} 数据可视化")
            title.setPos(scene_width / 2 - 100, 10)
            self.visualization_scene.addItem(title)
            
            self.sql_result_tab.setCurrentIndex(2)
            
        except Exception as e:
            # 可视化失败时不显示错误，保持空白
            pass
    
    def explain_sql(self):
        """解释SQL执行计划"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            QMessageBox.warning(self, "警告", "请输入SQL语句")
            return
        
        if not sql.lower().startswith("select"):
            QMessageBox.warning(self, "警告", "EXPLAIN 只能用于SELECT语句")
            return
        
        try:
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {sql}")
            
            plan = cursor.fetchall()
            
            # 格式化执行计划
            formatted_plan = "执行计划:\n"
            for row in plan:
                formatted_plan += f"ID: {row[0]}, Parent: {row[1]}, NotUsed: {row[2]}, Detail: {row[3]}\n"
            
            self.sql_result_text.setPlainText(formatted_plan)
            self.sql_result_tab.setCurrentIndex(1)
            
            self.status_bar.showMessage("执行计划生成成功")
            
        except Exception as e:
            error_msg = f"生成执行计划失败:\n{str(e)}"
            QMessageBox.critical(self, "错误", error_msg)
            self.sql_result_text.setPlainText(error_msg)
            self.sql_result_tab.setCurrentIndex(1)
            self.status_bar.showMessage("执行计划生成失败")
    
    def format_sql(self):
        """格式化SQL语句"""
        sql = self.sql_editor.toPlainText().strip()
        if not sql:
            return
        
        # 简单的SQL格式化
        formatted_sql = sql
        keywords = [
            "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
            "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "VIEW", "TRIGGER",
            "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",
            "NULL", "NOT", "AND", "OR", "AS", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
            "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "UNION", "ALL",
            "DISTINCT", "EXISTS", "BETWEEN", "LIKE", "IN", "IS", "ASC", "DESC", "BEGIN",
            "COMMIT", "ROLLBACK", "TRANSACTION", "EXPLAIN", "PRAGMA", "VACUUM", "ANALYZE",
            "ATTACH", "DETACH", "DATABASE", "WITH", "RECURSIVE", "CASE", "WHEN", "THEN",
            "ELSE", "END", "CAST", "COLUMN", "CONSTRAINT", "TEMPORARY", "TEMP", "IF",
            "EXISTS", "REPLACE", "RENAME", "TO", "ADD", "COLUMN", "WITHOUT", "ROWID",
            "USING", "NATURAL", "CROSS", "INTERSECT", "EXCEPT", "COLLATE", "GLOB",
            "REGEXP", "MATCH", "ESCAPE", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP"
        ]
        
        for keyword in keywords:
            formatted_sql = formatted_sql.replace(keyword, f"\n{keyword}")
            formatted_sql = formatted_sql.replace(keyword.lower(), f"\n{keyword}")
        
        # 移除多余的空行
        formatted_sql = "\n".join(line for line in formatted_sql.splitlines() if line.strip())
        
        self.sql_editor.setPlainText(formatted_sql.strip())
    
    def clear_sql(self):
        """清除SQL编辑器和结果"""
        self.sql_editor.clear()
        self.sql_result_table.setModel(QStandardItemModel())
        self.sql_result_text.clear()
        self.visualization_scene.clear()
    
    def show_sql_history(self):
        """显示SQL历史记录对话框"""
        if not hasattr(self, 'sql_history') or not self.sql_history:
            QMessageBox.information(self, "信息", "没有历史记录")
            return
        
        dialog = QDialog(self)
        dialog.setWindowTitle("SQL历史记录")
        dialog.resize(600, 400)
        
        layout = QVBoxLayout()
        dialog.setLayout(layout)
        
        list_widget = QListWidget()
        for sql in reversed(self.sql_history):
            item = QListWidgetItem(sql[:100] + "..." if len(sql) > 100 else sql)
            item.setData(Qt.UserRole, sql)
            list_widget.addItem(item)
        
        layout.addWidget(list_widget)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        def load_selected_sql():
            selected_items = list_widget.selectedItems()
            if selected_items:
                sql = selected_items[0].data(Qt.UserRole)
                self.sql_editor.setPlainText(sql)
        
        list_widget.itemDoubleClicked.connect(load_selected_sql)
        list_widget.itemDoubleClicked.connect(dialog.accept)
        
        if dialog.exec_() == QDialog.Accepted:
            selected_items = list_widget.selectedItems()
            if selected_items:
                sql = selected_items[0].data(Qt.UserRole)
                self.sql_editor.setPlainText(sql)
    
    def update_completer(self):
        """更新SQL自动完成"""
        if not hasattr(self, 'completer'):
            return
        
        # 获取当前光标前的文本
        cursor = self.sql_editor.textCursor()
        cursor.select(QTextCursor.WordUnderCursor)
        word = cursor.selectedText()
        
        if word:
            # 获取当前数据库的表和列名
            if self.current_db_path and self.current_db_path in self.open_databases:
                try:
                    conn = self.open_databases[self.current_db_path]
                    cursor = conn.cursor()
                    
                    # 获取所有表名
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    # 获取所有列名
                    columns = []
                    for table in tables:
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns.extend([col[1] for col in cursor.fetchall()])
                    
                    # 更新自动完成模型
                    model = QStringListModel()
                    model.setStringList(tables + columns + [
                        "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
                        "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "VIEW", "TRIGGER",
                        "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",
                        "NULL", "NOT", "AND", "OR", "AS", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
                        "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "ON", "UNION", "ALL",
                        "DISTINCT", "EXISTS", "BETWEEN", "LIKE", "IN", "IS", "ASC", "DESC", "BEGIN",
                        "COMMIT", "ROLLBACK", "TRANSACTION", "EXPLAIN", "PRAGMA", "VACUUM", "ANALYZE"
                    ])
                    
                    self.completer.setModel(model)
                    
                except:
                    pass
    
    def show_table_context_menu(self, pos, view, table_name, db_path):
        """显示表数据上下文菜单"""
        menu = QMenu()
        
        edit_action = QAction(QIcon.fromTheme("document-edit"), "编辑记录", self)
        edit_action.triggered.connect(lambda: self.edit_record(table_name, view, db_path))
        menu.addAction(edit_action)
        
        delete_action = QAction(QIcon.fromTheme("edit-delete"), "删除记录", self)
        delete_action.triggered.connect(lambda: self.delete_records(table_name, view, db_path))
        menu.addAction(delete_action)
        
        menu.addSeparator()
        
        copy_action = QAction(QIcon.fromTheme("edit-copy"), "复制选中内容", self)
        copy_action.triggered.connect(lambda: self.copy_selected_content(view))
        menu.addAction(copy_action)
        
        menu.exec_(view.viewport().mapToGlobal(pos))
    
    def show_sql_result_context_menu(self, pos):
        """显示SQL结果上下文菜单"""
        menu = QMenu()
        
        copy_action = QAction(QIcon.fromTheme("edit-copy"), "复制选中内容", self)
        copy_action.triggered.connect(lambda: self.copy_selected_content(self.sql_result_table))
        menu.addAction(copy_action)
        
        export_action = QAction(QIcon.fromTheme("document-save-as"), "导出结果...", self)
        export_action.triggered.connect(self.export_query_results)
        menu.addAction(export_action)
        
        visualize_action = QAction(QIcon.fromTheme("image-x-generic"), "可视化数据", self)
        visualize_action.triggered.connect(self.visualize_current_result)
        menu.addAction(visualize_action)
        
        menu.exec_(self.sql_result_table.viewport().mapToGlobal(pos))
    
    def visualize_current_result(self):
        """可视化当前结果"""
        model = self.sql_result_table.model()
        if not model or model.rowCount() == 0:
            QMessageBox.warning(self, "警告", "没有可可视化的数据")
            return
        
        # 获取数据和列名
        data = []
        column_names = []
        
        if isinstance(model, QSortFilterProxyModel):
            source_model = model.sourceModel()
        else:
            source_model = model
        
        # 获取列名
        column_names = [source_model.headerData(col, Qt.Horizontal) for col in range(source_model.columnCount())]
        
        # 获取数据
        for row in range(source_model.rowCount()):
            row_data = []
            for col in range(source_model.columnCount()):
                index = source_model.index(row, col)
                value = source_model.data(index)
                row_data.append(value)
            data.append(row_data)
        
        # 可视化数据
        self.visualize_data(data, column_names)
        self.sql_result_tab.setCurrentIndex(2)
    
    def copy_selected_content(self, view):
        """复制选中内容"""
        selection = view.selectionModel()
        if not selection.hasSelection():
            return
        
        selected_indexes = view.selectionModel().selectedIndexes()
        if not selected_indexes:
            return
        
        # 获取行和列范围
        rows = sorted(set(index.row() for index in selected_indexes))
        cols = sorted(set(index.column() for index in selected_indexes))
        
        # 获取表头
        model = view.model()
        headers = [model.headerData(col, Qt.Horizontal) for col in cols]
        
        # 获取数据
        data = []
        for row in rows:
            row_data = []
            for col in cols:
                index = model.index(row, col)
                row_data.append(str(model.data(index)))
            data.append(row_data)
        
        # 格式化为文本
        text = "\t".join(headers) + "\n"
        for row in data:
            text += "\t".join(row) + "\n"
        
        # 复制到剪贴板
        clipboard = QApplication.clipboard()
        clipboard.setText(text.strip())
    
    def paste_content(self):
        """粘贴内容到SQL编辑器"""
        clipboard = QApplication.clipboard()
        text = clipboard.text()
        if text:
            self.sql_editor.insertPlainText(text)
    
    def export_query_results(self):
        """导出查询结果"""
        model = self.sql_result_table.model()
        if not model or model.rowCount() == 0:
            QMessageBox.warning(self, "警告", "没有可导出的数据")
            return
        
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出查询结果", "", 
            "CSV文件 (*.csv);;文本文件 (*.txt);;所有文件 (*)", 
            options=options
        )
        
        if file_path:
            try:
                with open(file_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    
                    # 写入表头
                    headers = [model.headerData(col, Qt.Horizontal) for col in range(model.columnCount())]
                    writer.writerow(headers)
                    
                    # 写入数据
                    for row in range(model.rowCount()):
                        row_data = []
                        for col in range(model.columnCount()):
                            index = model.index(row, col)
                            value = model.data(index)
                            row_data.append(value if value is not None else "")
                        
                        writer.writerow(row_data)
                
                QMessageBox.information(self, "成功", f"查询结果已导出到 {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导出失败:\n{str(e)}")
    
    def create_table_dialog(self):
        """创建表对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        dialog = QDialog(self)
        dialog.setWindowTitle("创建新表")
        dialog.resize(800, 600)
        
        layout = QVBoxLayout()
        dialog.setLayout(layout)
        
        # 表名输入
        form_layout = QFormLayout()
        
        self.table_name_edit = QLineEdit()
        form_layout.addRow("表名:", self.table_name_edit)
        
        # 列定义区域
        self.columns_widget = QWidget()
        columns_layout = QVBoxLayout()
        self.columns_widget.setLayout(columns_layout)
        
        # 添加列按钮
        add_column_button = QPushButton(QIcon.fromTheme("list-add"), "添加列")
        add_column_button.clicked.connect(self.add_column_definition)
        columns_layout.addWidget(add_column_button)
        
        # 初始列
        self.column_definitions = []
        self.add_column_definition()
        
        form_layout.addRow("列定义:", self.columns_widget)
        layout.addLayout(form_layout)
        
        # SQL预览
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True)
        self.sql_preview.setFont(QFont("Consolas", 10))
        layout.addWidget(QLabel("SQL预览:"))
        layout.addWidget(self.sql_preview)
        
        # 按钮区域
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(lambda: self.create_table(dialog))
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        # 连接信号更新SQL预览
        self.table_name_edit.textChanged.connect(self.update_sql_preview)
        for widget in self.columns_widget.findChildren(QWidget):
            if isinstance(widget, (QLineEdit, QComboBox, QCheckBox)):
                widget.textChanged.connect(self.update_sql_preview) if hasattr(widget, 'textChanged') else widget.stateChanged.connect(self.update_sql_preview)
        
        dialog.exec_()
    
    def add_column_definition(self):
        """添加列定义"""
        column_widget = QWidget()
        column_layout = QHBoxLayout()
        column_widget.setLayout(column_layout)
        
        name_edit = QLineEdit()
        name_edit.setPlaceholderText("列名")
        name_edit.textChanged.connect(self.update_sql_preview)
        column_layout.addWidget(name_edit)
        
        type_combo = QComboBox()
        type_combo.addItems([
            "INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC", 
            "BOOLEAN", "DATE", "DATETIME", "VARCHAR(255)"
        ])
        type_combo.currentTextChanged.connect(self.update_sql_preview)
        column_layout.addWidget(type_combo)
        
        pk_check = QCheckBox("主键")
        pk_check.stateChanged.connect(self.update_sql_preview)
        column_layout.addWidget(pk_check)
        
        nn_check = QCheckBox("非空")
        nn_check.stateChanged.connect(self.update_sql_preview)
        column_layout.addWidget(nn_check)
        
        ai_check = QCheckBox("自增")
        ai_check.stateChanged.connect(self.update_sql_preview)
        column_layout.addWidget(ai_check)
        
        default_label = QLabel("默认值:")
        default_edit = QLineEdit()
        default_edit.setPlaceholderText("可选")
        default_edit.textChanged.connect(self.update_sql_preview)
        column_layout.addWidget(default_label)
        column_layout.addWidget(default_edit)
        
        remove_button = QPushButton(QIcon.fromTheme("list-remove"), "")
        remove_button.clicked.connect(lambda: self.remove_column_definition(column_widget))
        column_layout.addWidget(remove_button)
        
        self.column_definitions.append({
            'widget': column_widget,
            'name_edit': name_edit,
            'type_combo': type_combo,
            'pk_check': pk_check,
            'nn_check': nn_check,
            'ai_check': ai_check,
            'default_edit': default_edit
        })
        
        self.columns_widget.layout().insertWidget(
            self.columns_widget.layout().count() - 1, 
            column_widget
        )
        
        self.update_sql_preview()
    
    def remove_column_definition(self, widget):
        """移除列定义"""
        for i, col_def in enumerate(self.column_definitions):
            if col_def['widget'] == widget:
                self.column_definitions.pop(i)
                widget.deleteLater()
                break
        
        self.update_sql_preview()
    
    def update_sql_preview(self):
        """更新SQL预览"""
        table_name = self.table_name_edit.text().strip()
        if not table_name:
            self.sql_preview.setPlainText("")
            return
        
        sql = f"CREATE TABLE {table_name} (\n"
        
        columns = []
        primary_keys = []
        
        for col_def in self.column_definitions:
            col_name = col_def['name_edit'].text().strip()
            if not col_name:
                continue
            
            col_type = col_def['type_combo'].currentText()
            
            col_sql = f"    {col_name} {col_type}"
            
            if col_def['nn_check'].isChecked():
                col_sql += " NOT NULL"
            
            if col_def['ai_check'].isChecked():
                col_sql += " AUTOINCREMENT"
            
            default_value = col_def['default_edit'].text().strip()
            if default_value:
                col_sql += f" DEFAULT {default_value}"
            
            if col_def['pk_check'].isChecked():
                primary_keys.append(col_name)
            
            columns.append(col_sql)
        
        sql += ",\n".join(columns)
        
        if primary_keys:
            sql += ",\n    PRIMARY KEY (" + ", ".join(primary_keys) + ")"
        
        sql += "\n);"
        
        self.sql_preview.setPlainText(sql)
    
    def create_table(self, dialog):
        """创建表"""
        table_name = self.table_name_edit.text().strip()
        if not table_name:
            QMessageBox.warning(self, "警告", "请输入表名")
            return
        
        if not self.column_definitions:
            QMessageBox.warning(self, "警告", "请至少定义一个列")
            return
        
        # 检查列名是否有效
        for col_def in self.column_definitions:
            col_name = col_def['name_edit'].text().strip()
            if not col_name:
                QMessageBox.warning(self, "警告", "所有列必须有名")
                return
            
            if not col_name.replace("_", "").isalnum():
                QMessageBox.warning(self, "警告", f"列名 '{col_name}' 只能包含字母、数字和下划线")
                return
        
        # 检查主键是否有效
        primary_keys = [col_def['name_edit'].text().strip() for col_def in self.column_definitions if col_def['pk_check'].isChecked()]
        if not primary_keys:
            reply = QMessageBox.question(
                self, "确认", 
                "没有定义主键，确定要继续吗?", 
                QMessageBox.Yes | QMessageBox.No
            )
            if reply != QMessageBox.Yes:
                return
        
        # 构建SQL语句
        sql = self.sql_preview.toPlainText()
        
        try:
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            cursor.execute(sql)
            conn.commit()
            
            # 刷新当前数据库
            current_index = self.db_tab_widget.currentIndex()
            if current_index >= 0:
                self.db_tab_widget.widget(current_index).load_tables()
            
            dialog.accept()
            
            QMessageBox.information(self, "成功", f"表 {table_name} 创建成功")
            
        except Exception as e:
            conn.rollback()
            QMessageBox.critical(self, "错误", f"创建表失败:\n{str(e)}")
    
    def drop_table_dialog(self):
        """删除表对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        try:
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            
            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            
            if not tables:
                QMessageBox.information(self, "信息", "数据库中没有用户表")
                return
            
            table, ok = QInputDialog.getItem(
                self, "删除表", "选择要删除的表:", tables, 0, False
            )
            
            if ok and table:
                reply = QMessageBox.question(
                    self, "确认", 
                    f"确定要删除表 {table} 吗? 此操作不可恢复!", 
                    QMessageBox.Yes | QMessageBox.No
                )
                
                if reply == QMessageBox.Yes:
                    try:
                        cursor.execute("BEGIN")
                        cursor.execute(f"DROP TABLE {table}")
                        conn.commit()
                        
                        # 刷新当前数据库
                        current_index = self.db_tab_widget.currentIndex()
                        if current_index >= 0:
                            self.db_tab_widget.widget(current_index).load_tables()
                        
                        QMessageBox.information(self, "成功", f"表 {table} 已删除")
                        
                    except Exception as e:
                        conn.rollback()
                        QMessageBox.critical(self, "错误", f"删除表失败:\n{str(e)}")
        
        except Exception as e:
            QMessageBox.critical(self, "错误", f"获取表列表失败:\n{str(e)}")
    
    def manage_indexes(self, table_name, db_path):
        """管理索引"""
        if db_path not in self.open_databases:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f"管理索引 - {table_name}")
        dialog.resize(600, 400)
        
        layout = QVBoxLayout()
        dialog.setLayout(layout)
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取现有索引
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = cursor.fetchall()
            
            # 索引列表
            index_list = QTableView()
            model = QStandardItemModel()
            model.setHorizontalHeaderLabels(["名称", "唯一", "SQL"])
            
            for idx in indexes:
                name = QStandardItem(idx[1])
                unique = QStandardItem("是" if idx[2] else "否")
                
                # 获取索引SQL
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='index' AND name='{idx[1]}'")
                result = cursor.fetchone()
                sql = result[0] if result else ""
                
                sql_item = QStandardItem(sql)
                
                model.appendRow([name, unique, sql_item])
            
            index_list.setModel(model)
            index_list.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            layout.addWidget(index_list)
            
            # 按钮区域
            button_layout = QHBoxLayout()
            
            create_button = QPushButton("创建索引...")
            create_button.clicked.connect(lambda: self.create_index_dialog(table_name, db_path, dialog))
            button_layout.addWidget(create_button)
            
            drop_button = QPushButton("删除索引")
            drop_button.clicked.connect(lambda: self.drop_index(table_name, db_path, index_list, dialog))
            button_layout.addWidget(drop_button)
            
            button_layout.addStretch()
            
            close_button = QPushButton("关闭")
            close_button.clicked.connect(dialog.accept)
            button_layout.addWidget(close_button)
            
            layout.addLayout(button_layout)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"管理索引失败:\n{str(e)}")
    
    def create_index_dialog(self, table_name, db_path, parent_dialog):
        """创建索引对话框"""
        if db_path not in self.open_databases:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        dialog = QDialog(self)
        dialog.setWindowTitle(f"为 {table_name} 创建索引")
        dialog.resize(500, 300)
        
        layout = QVBoxLayout()
        dialog.setLayout(layout)
        
        form_layout = QFormLayout()
        
        # 索引名称
        index_name_edit = QLineEdit()
        index_name_edit.setPlaceholderText(f"idx_{table_name}_column")
        form_layout.addRow("索引名称:", index_name_edit)
        
        # 唯一索引
        unique_check = QCheckBox("唯一索引")
        form_layout.addRow(unique_check)
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表列
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # 列选择
            columns_list = QListWidget()
            columns_list.setSelectionMode(QAbstractItemView.MultiSelection)
            columns_list.addItems(columns)
            form_layout.addRow("选择列:", columns_list)
            
            layout.addLayout(form_layout)
            
            # SQL预览
            sql_preview = QTextEdit()
            sql_preview.setReadOnly(True)
            sql_preview.setFont(QFont("Consolas", 10))
            layout.addWidget(QLabel("SQL预览:"))
            layout.addWidget(sql_preview)
            
            def update_sql_preview():
                name = index_name_edit.text().strip() or f"idx_{table_name}_{'_'.join(col for col in columns if col)}"
                unique = "UNIQUE " if unique_check.isChecked() else ""
                selected_cols = [item.text() for item in columns_list.selectedItems()]
                
                if not selected_cols:
                    sql = ""
                else:
                    sql = f"CREATE {unique}INDEX {name} ON {table_name} ({', '.join(selected_cols)})"
                
                sql_preview.setPlainText(sql)
            
            index_name_edit.textChanged.connect(update_sql_preview)
            unique_check.stateChanged.connect(update_sql_preview)
            columns_list.itemSelectionChanged.connect(update_sql_preview)
            
            # 按钮区域
            button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            button_box.accepted.connect(lambda: self.create_index(table_name, db_path, sql_preview.toPlainText(), dialog, parent_dialog))
            button_box.rejected.connect(dialog.reject)
            layout.addWidget(button_box)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"创建索引对话框失败:\n{str(e)}")
    
    def create_index(self, table_name, db_path, sql, dialog, parent_dialog):
        """创建索引"""
        if not sql:
            QMessageBox.warning(self, "警告", "无效的索引定义")
            return
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            cursor.execute(sql)
            conn.commit()
            
            dialog.accept()
            parent_dialog.accept()
            
            QMessageBox.information(self, "成功", "索引创建成功")
            self.manage_indexes(table_name, db_path)
            
        except Exception as e:
            conn.rollback()
            QMessageBox.critical(self, "错误", f"创建索引失败:\n{str(e)}")
    
    def drop_index(self, table_name, db_path, index_list, parent_dialog):
        """删除索引"""
        selection = index_list.selectionModel()
        if not selection.hasSelection():
            QMessageBox.warning(self, "警告", "请选择要删除的索引")
            return
        
        selected_rows = set(index.row() for index in selection.selectedIndexes())
        if len(selected_rows) != 1:
            QMessageBox.warning(self, "警告", "请选择单个索引进行删除")
            return
        
        row = selected_rows.pop()
        model = index_list.model()
        index_name = model.data(model.index(row, 0))
        
        reply = QMessageBox.question(
            self, "确认", 
            f"确定要删除索引 {index_name} 吗?", 
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                conn = self.open_databases[db_path]
                cursor = conn.cursor()
                cursor.execute("BEGIN")
                cursor.execute(f"DROP INDEX {index_name}")
                conn.commit()
                
                parent_dialog.accept()
                self.manage_indexes(table_name, db_path)
                
                QMessageBox.information(self, "成功", f"索引 {index_name} 已删除")
                
            except Exception as e:
                conn.rollback()
                QMessageBox.critical(self, "错误", f"删除索引失败:\n{str(e)}")
    
    def add_record(self, table_name, db_path):
        """添加记录"""
        if db_path not in self.open_databases:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            dialog = QDialog(self)
            dialog.setWindowTitle(f"添加记录到 {table_name}")
            dialog.resize(500, 400)
            
            layout = QVBoxLayout()
            dialog.setLayout(layout)
            
            form_layout = QFormLayout()
            
            self.record_edits = {}
            
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                label = QLabel(col_name)
                
                # 根据列类型选择合适的输入控件
                if "INT" in col_type.upper():
                    edit = QSpinBox()
                    edit.setRange(-2147483648, 2147483647)
                elif "REAL" in col_type.upper() or "FLOAT" in col_type.upper() or "DOUBLE" in col_type.upper():
                    edit = QDoubleSpinBox()
                    edit.setRange(-1.7e308, 1.7e308)
                    edit.setDecimals(4)
                elif "DATE" in col_type.upper():
                    edit = QDateEdit()
                    edit.setCalendarPopup(True)
                    edit.setDate(datetime.now().date())
                elif "TIME" in col_type.upper() or "DATETIME" in col_type.upper() or "TIMESTAMP" in col_type.upper():
                    edit = QDateTimeEdit()
                    edit.setCalendarPopup(True)
                    edit.setDateTime(datetime.now())
                else:  # 默认为文本输入
                    edit = QLineEdit()
                
                edit.setPlaceholderText(f"{col_type} 类型")
                
                if col[5] == 1 and "AUTOINCREMENT" in col[2].upper():
                    edit.setEnabled(False)
                    if isinstance(edit, (QLineEdit, QSpinBox, QDoubleSpinBox)):
                        edit.setValue("自动生成")
                
                form_layout.addRow(label, edit)
                self.record_edits[col_name] = edit
            
            layout.addLayout(form_layout)
            
            # SQL预览
            self.sql_preview = QTextEdit()
            self.sql_preview.setReadOnly(True)
            self.sql_preview.setFont(QFont("Consolas", 10))
            layout.addWidget(QLabel("SQL预览:"))
            layout.addWidget(self.sql_preview)
            
            # 连接信号更新SQL预览
            for edit in self.record_edits.values():
                if isinstance(edit, QLineEdit):
                    edit.textChanged.connect(self.update_insert_sql_preview)
                elif isinstance(edit, (QSpinBox, QDoubleSpinBox)):
                    edit.valueChanged.connect(self.update_insert_sql_preview)
                elif isinstance(edit, (QDateEdit, QDateTimeEdit)):
                    edit.dateTimeChanged.connect(self.update_insert_sql_preview)
            
            self.update_insert_sql_preview()
            
            # 按钮区域
            button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            button_box.accepted.connect(lambda: self.insert_record(table_name, db_path, columns, dialog))
            button_box.rejected.connect(dialog.reject)
            layout.addWidget(button_box)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法添加记录:\n{str(e)}")
    
    def update_insert_sql_preview(self):
        """更新插入SQL预览"""
        if not hasattr(self, 'record_edits'):
            return
        
        values = []
        for edit in self.record_edits.values():
            if isinstance(edit, QLineEdit):
                text = edit.text()
                if text == "自动生成":
                    values.append("NULL")
                elif not text:
                    values.append("NULL")
                else:
                    # 安全处理字符串中的引号
                    escaped_text = text.replace("'", "''")
                    values.append(f"'{escaped_text}'")
            elif isinstance(edit, (QSpinBox, QDoubleSpinBox)):
                values.append(str(edit.value()))
            elif isinstance(edit, QDateEdit):
                values.append(f"'{edit.date().toString('yyyy-MM-dd')}'")
            elif isinstance(edit, QDateTimeEdit):
                values.append(f"'{edit.dateTime().toString('yyyy-MM-dd hh:mm:ss')}'")
        
        # 从对话框标题获取表名
        title = self.sender().parent().window().windowTitle()
        if "到 " in title:
            table_name = title.split("到 ")[-1]
            sql = f"INSERT INTO {table_name} VALUES ({', '.join(values)})"
            
            if hasattr(self, 'sql_preview'):
                self.sql_preview.setPlainText(sql)
    
    def insert_record(self, table_name, db_path, columns, dialog):
        """插入记录"""
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            
            # 构建列名和值
            col_names = []
            values = []
            
            for col in columns:
                col_name = col[1]
                edit = self.record_edits[col_name]
                
                if col[5] == 1 and "AUTOINCREMENT" in col[2].upper():
                    continue
                
                col_names.append(col_name)
                
                if isinstance(edit, QLineEdit):
                    text = edit.text()
                    if not text or text == "自动生成":
                        values.append(None)
                    else:
                        values.append(text)
                elif isinstance(edit, QSpinBox):
                    values.append(edit.value())
                elif isinstance(edit, QDoubleSpinBox):
                    values.append(edit.value())
                elif isinstance(edit, QDateEdit):
                    values.append(edit.date().toString('yyyy-MM-dd'))
                elif isinstance(edit, QDateTimeEdit):
                    values.append(edit.dateTime().toString('yyyy-MM-dd hh:mm:ss'))
            
            # 构建SQL
            sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(['?']*len(col_names))})"
            
            cursor.execute(sql, values)
            conn.commit()
            
            dialog.accept()
            
            # 刷新当前数据库
            current_index = self.db_tab_widget.currentIndex()
            if current_index >= 0:
                self.db_tab_widget.widget(current_index).load_tables()
            
            QMessageBox.information(self, "成功", "记录添加成功")
            
        except Exception as e:
            conn.rollback()
            QMessageBox.critical(self, "错误", f"添加记录失败:\n{str(e)}")
    
    def edit_record(self, table_name, view, db_path):
        """编辑记录"""
        if db_path not in self.open_databases:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        selection = view.selectionModel()
        if not selection.hasSelection():
            QMessageBox.warning(self, "警告", "请选择要编辑的记录")
            return
        
        selected_rows = set(index.row() for index in selection.selectedIndexes())
        if len(selected_rows) != 1:
            QMessageBox.warning(self, "警告", "请选择单条记录进行编辑")
            return
        
        row = selected_rows.pop()
        model = view.model().sourceModel() if isinstance(view.model(), QSortFilterProxyModel) else view.model()
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # 获取主键列
            primary_keys = [col[1] for col in columns if col[5] == 1]
            if not primary_keys:
                QMessageBox.warning(self, "警告", "无法编辑: 表没有主键")
                return
            
            dialog = QDialog(self)
            dialog.setWindowTitle(f"编辑 {table_name} 中的记录")
            dialog.resize(500, 400)
            
            layout = QVBoxLayout()
            dialog.setLayout(layout)
            
            form_layout = QFormLayout()
            
            self.record_edits = {}
            pk_values = {}
            
            for i, col in enumerate(columns):
                col_name = col[1]
                col_type = col[2]
                
                label = QLabel(col_name)
                
                # 根据列类型选择合适的输入控件
                if "INT" in col_type.upper():
                    edit = QSpinBox()
                    edit.setRange(-2147483648, 2147483647)
                    edit.setValue(int(model.data(model.index(row, i))) if model.data(model.index(row, i)) else 0)
                elif "REAL" in col_type.upper() or "FLOAT" in col_type.upper() or "DOUBLE" in col_type.upper():
                    edit = QDoubleSpinBox()
                    edit.setRange(-1.7e308, 1.7e308)
                    edit.setDecimals(4)
                    edit.setValue(float(model.data(model.index(row, i)))) if model.data(model.index(row, i)) else 0.0
                elif "DATE" in col_type.upper():
                    edit = QDateEdit()
                    edit.setCalendarPopup(True)
                    date_str = model.data(model.index(row, i))
                    if date_str:
                        edit.setDate(QDate.fromString(date_str, 'yyyy-MM-dd'))
                elif "TIME" in col_type.upper() or "DATETIME" in col_type.upper() or "TIMESTAMP" in col_type.upper():
                    edit = QDateTimeEdit()
                    edit.setCalendarPopup(True)
                    datetime_str = model.data(model.index(row, i))
                    if datetime_str:
                        edit.setDateTime(QDateTime.fromString(datetime_str, 'yyyy-MM-dd hh:mm:ss'))
                else:  # 默认为文本输入
                    edit = QLineEdit()
                    edit.setText(str(model.data(model.index(row, i))) if model.data(model.index(row, i)) else "")
                
                if col[5] == 1:  # 主键
                    edit.setEnabled(False)
                    pk_values[col_name] = model.data(model.index(row, i))
                
                form_layout.addRow(label, edit)
                self.record_edits[col_name] = edit
            
            layout.addLayout(form_layout)
            
            # SQL预览
            self.sql_preview = QTextEdit()
            self.sql_preview.setReadOnly(True)
            self.sql_preview.setFont(QFont("Consolas", 10))
            layout.addWidget(QLabel("SQL预览:"))
            layout.addWidget(self.sql_preview)
            
            # 连接信号更新SQL预览
            for col_name, edit in self.record_edits.items():
                if col_name in primary_keys:
                    continue
                
                if isinstance(edit, QLineEdit):
                    edit.textChanged.connect(lambda: self.update_update_sql_preview(table_name, primary_keys, pk_values))
                elif isinstance(edit, (QSpinBox, QDoubleSpinBox)):
                    edit.valueChanged.connect(lambda: self.update_update_sql_preview(table_name, primary_keys, pk_values))
                elif isinstance(edit, (QDateEdit, QDateTimeEdit)):
                    edit.dateTimeChanged.connect(lambda: self.update_update_sql_preview(table_name, primary_keys, pk_values))
            
            self.update_update_sql_preview(table_name, primary_keys, pk_values)
            
            # 按钮区域
            button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            button_box.accepted.connect(lambda: self.update_record(table_name, db_path, primary_keys, pk_values, dialog))
            button_box.rejected.connect(dialog.reject)
            layout.addWidget(button_box)
            
            dialog.exec_()
            
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法编辑记录:\n{str(e)}")
    
    def update_update_sql_preview(self, table_name, primary_keys, pk_values):
        """更新更新SQL预览"""
        if not hasattr(self, 'record_edits'):
            return
        
        # 构建SET子句
        set_clause = []
        for col_name, edit in self.record_edits.items():
            if col_name in primary_keys:
                continue
            
            if edit.isEnabled():
                if isinstance(edit, QLineEdit):
                    value = edit.text()
                    # 安全处理字符串中的引号
                    escaped_value = value.replace("'", "''")
                    set_clause.append(f"{col_name} = '{escaped_value}'")
                elif isinstance(edit, (QSpinBox, QDoubleSpinBox)):
                    set_clause.append(f"{col_name} = {edit.value()}")
                elif isinstance(edit, QDateEdit):
                    set_clause.append(f"{col_name} = '{edit.date().toString('yyyy-MM-dd')}'")
                elif isinstance(edit, QDateTimeEdit):
                    set_clause.append(f"{col_name} = '{edit.dateTime().toString('yyyy-MM-dd hh:mm:ss')}'")
        
        # 构建WHERE子句
        where_clause = []
        for pk in primary_keys:
            pk_value = pk_values[pk]
            # 安全处理主键值中的引号
            if isinstance(pk_value, str):
                escaped_pk_value = pk_value.replace("'", "''")
                where_clause.append(f"{pk} = '{escaped_pk_value}'")
            else:
                where_clause.append(f"{pk} = {pk_value}")
        
        sql = f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE {' AND '.join(where_clause)}"
        
        if hasattr(self, 'sql_preview'):
            self.sql_preview.setPlainText(sql)
    
    def update_record(self, table_name, db_path, primary_keys, pk_values, dialog):
        """更新记录"""
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            cursor.execute("BEGIN")
            
            # 构建SET子句
            set_clause = []
            values = []
            
            for col_name, edit in self.record_edits.items():
                if col_name in primary_keys:
                    continue
                
                if edit.isEnabled():
                    set_clause.append(f"{col_name} = ?")
                    
                    if isinstance(edit, QLineEdit):
                        values.append(edit.text())
                    elif isinstance(edit, QSpinBox):
                        values.append(edit.value())
                    elif isinstance(edit, QDoubleSpinBox):
                        values.append(edit.value())
                    elif isinstance(edit, QDateEdit):
                        values.append(edit.date().toString('yyyy-MM-dd'))
                    elif isinstance(edit, QDateTimeEdit):
                        values.append(edit.dateTime().toString('yyyy-MM-dd hh:mm:ss'))
            
            # 构建WHERE子句
            where_clause = []
            for pk in primary_keys:
                where_clause.append(f"{pk} = ?")
                values.append(pk_values[pk])
            
            # 构建SQL
            sql = f"UPDATE {table_name} SET {', '.join(set_clause)} WHERE {' AND '.join(where_clause)}"
            
            cursor.execute(sql, values)
            conn.commit()
            
            dialog.accept()
            
            # 刷新当前数据库
            current_index = self.db_tab_widget.currentIndex()
            if current_index >= 0:
                self.db_tab_widget.widget(current_index).load_tables()
            
            QMessageBox.information(self, "成功", "记录更新成功")
            
        except Exception as e:
            conn.rollback()
            QMessageBox.critical(self, "错误", f"更新记录失败:\n{str(e)}")
    
    def delete_records(self, table_name, view, db_path):
        """删除记录"""
        if db_path not in self.open_databases:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        selection = view.selectionModel()
        if not selection.hasSelection():
            QMessageBox.warning(self, "警告", "请选择要删除的记录")
            return
        
        selected_rows = set(index.row() for index in selection.selectedIndexes())
        model = view.model().sourceModel() if isinstance(view.model(), QSortFilterProxyModel) else view.model()
        
        try:
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # 获取主键列
            primary_keys = [col[1] for col in columns if col[5] == 1]
            if not primary_keys:
                QMessageBox.warning(self, "警告", "无法删除: 表没有主键")
                return
            
            # 确认删除
            reply = QMessageBox.question(
                self, "确认", 
                f"确定要删除选中的 {len(selected_rows)} 条记录吗?", 
                QMessageBox.Yes | QMessageBox.No
            )
            
            if reply != QMessageBox.Yes:
                return
            
            # 构建WHERE子句
            where_clauses = []
            all_values = []
            
            for row in selected_rows:
                where_parts = []
                values = []
                
                for pk in primary_keys:
                    col_index = next(i for i, col in enumerate(columns) if col[1] == pk)
                    where_parts.append(f"{pk} = ?")
                    values.append(model.data(model.index(row, col_index)))
                
                where_clauses.append("(" + " AND ".join(where_parts) + ")")
                all_values.extend(values)
            
            # 构建SQL
            sql = f"DELETE FROM {table_name} WHERE {' OR '.join(where_clauses)}"
            
            cursor.execute("BEGIN")
            cursor.execute(sql, all_values)
            conn.commit()
            
            # 刷新当前数据库
            current_index = self.db_tab_widget.currentIndex()
            if current_index >= 0:
                self.db_tab_widget.widget(current_index).load_tables()
            
            QMessageBox.information(self, "成功", f"已删除 {len(selected_rows)} 条记录")
            
        except Exception as e:
            conn.rollback()
            QMessageBox.critical(self, "错误", f"删除记录失败:\n{str(e)}")
    
    def export_data_dialog(self):
        """导出数据对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出数据", 
            self.settings.value("lastExportDir", ""),
            "SQL文件 (*.sql);;CSV文件 (*.csv);;所有文件 (*)", 
            options=options
        )
        
        if file_path:
            self.settings.setValue("lastExportDir", os.path.dirname(file_path))
            try:
                if file_path.endswith('.sql'):
                    self.export_to_sql(file_path)
                else:
                    self.export_to_csv(file_path)
                
                QMessageBox.information(self, "成功", f"数据已导出到 {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导出失败:\n{str(e)}")
    
    def export_to_sql(self, file_path):
        """导出为SQL文件"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("-- SQLite 数据库导出\n")
            f.write(f"-- 导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- 数据库: {self.current_db_path}\n\n")
            
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            
            # 导出表结构
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables_result = cursor.fetchall()
            tables = [row[0] for row in tables_result] if tables_result else []
            
            for table in tables:
                # 获取表结构SQL
                cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
                result = cursor.fetchone()
                create_sql = result[0] if result else ""
                
                f.write(f"-- 表: {table}\n")
                f.write(f"{create_sql};\n\n")
                
                # 获取表数据
                cursor.execute(f"SELECT * FROM {table}")
                data = cursor.fetchall()
                
                if data:
                    # 获取列名
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns_result = cursor.fetchall()
                    columns = [col[1] for col in columns_result] if columns_result else []
                    
                    f.write(f"-- 数据: {table} ({len(data)} 行)\n")
                    
                    for row in data:
                        values = []
                        for value in row:
                            if value is None:
                                values.append("NULL")
                            elif isinstance(value, (int, float)):
                                values.append(str(value))
                            else:
                                escaped_value = str(value).replace("'", "''")
                                values.append(f"'{escaped_value}'")
                        
                        f.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
                    
                    f.write("\n")
            
            # 导出索引
            cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY name")
            indexes_result = cursor.fetchall()
            
            if indexes_result:
                f.write("-- 索引\n")
                for idx in indexes_result:
                    f.write(f"{idx[1]};\n")
                
                f.write("\n")
            
            # 导出视图
            cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name")
            views_result = cursor.fetchall()
            
            if views_result:
                f.write("-- 视图\n")
                for view in views_result:
                    f.write(f"{view[1]};\n")
                
                f.write("\n")
            
            # 导出触发器
            cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' ORDER BY name")
            triggers_result = cursor.fetchall()
            
            if triggers_result:
                f.write("-- 触发器\n")
                for trigger in triggers_result:
                    f.write(f"{trigger[1]};\n")
    
    def export_to_csv(self, file_path):
        """导出为CSV文件"""
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            conn = self.open_databases[self.current_db_path]
            cursor = conn.cursor()
            
            # 获取所有表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                # 写入表名
                writer.writerow([f"表: {table}"])
                
                # 获取表数据
                cursor.execute(f"SELECT * FROM {table}")
                data = cursor.fetchall()
                
                if data:
                    # 获取列名
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = [col[1] for col in cursor.fetchall()]
                    
                    # 写入列名
                    writer.writerow(columns)
                    
                    # 写入数据
                    for row in data:
                        writer.writerow(row)
                
                # 添加空行分隔表
                writer.writerow([])
    
    def export_table_data(self, table_name, db_path):
        """导出表数据"""
        if db_path not in self.open_databases:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getSaveFileName(
            self, f"导出表 {table_name} 数据", 
            f"{table_name}.csv",
            "CSV文件 (*.csv);;SQL文件 (*.sql);;所有文件 (*)", 
            options=options
        )
        
        if file_path:
            try:
                if file_path.endswith('.sql'):
                    self.export_table_to_sql(table_name, db_path, file_path)
                else:
                    self.export_table_to_csv(table_name, db_path, file_path)
                
                QMessageBox.information(self, "成功", f"表数据已导出到 {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导出失败:\n{str(e)}")
    
    def export_table_to_sql(self, table_name, db_path, file_path):
        """导出表为SQL文件"""
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("-- SQLite 表数据导出\n")
            f.write(f"-- 导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"-- 表: {table_name}\n")
            f.write(f"-- 数据库: {db_path}\n\n")
            
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表结构SQL
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            result = cursor.fetchone()
            create_sql = result[0] if result else ""
            
            f.write(f"{create_sql};\n\n")
            
            # 获取表数据
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            
            if data:
                # 获取列名
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns_result = cursor.fetchall()
                columns = [col[1] for col in columns_result] if columns_result else []
                
                f.write(f"-- 数据: {table_name} ({len(data)} 行)\n")
                
                for row in data:
                    values = []
                    for value in row:
                        if value is None:
                            values.append("NULL")
                        elif isinstance(value, (int, float)):
                            values.append(str(value))
                        else:
                            escaped_value = str(value).replace("'", "''")
                            values.append(f"'{escaped_value}'")
                    
                    f.write(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
    
    def export_table_to_csv(self, table_name, db_path, file_path):
        """导出表为CSV文件"""
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            
            conn = self.open_databases[db_path]
            cursor = conn.cursor()
            
            # 获取表数据
            cursor.execute(f"SELECT * FROM {table_name}")
            data = cursor.fetchall()
            
            if data:
                # 获取列名
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]
                
                # 写入列名
                writer.writerow(columns)
                
                # 写入数据
                for row in data:
                    writer.writerow(row)
    
    def import_data_dialog(self):
        """导入数据对话框"""
        if not self.current_db_path:
            QMessageBox.warning(self, "警告", "请先打开数据库")
            return
        
        options = QFileDialog.Options()
        file_path, _ = QFileDialog.getOpenFileName(
            self, "导入数据", 
            self.settings.value("lastImportDir", ""),
            "SQL文件 (*.sql);;CSV文件 (*.csv);;所有文件 (*)", 
            options=options
        )
        
        if file_path:
            self.settings.setValue("lastImportDir", os.path.dirname(file_path))
            try:
                if file_path.endswith('.sql'):
                    self.import_from_sql(file_path)
                else:
                    self.import_from_csv(file_path)
                
                # 刷新当前数据库
                current_index = self.db_tab_widget.currentIndex()
                if current_index >= 0:
                    self.db_tab_widget.widget(current_index).load_tables()
                
                QMessageBox.information(self, "成功", f"数据已从 {file_path} 导入")
                
            except Exception as e:
                QMessageBox.critical(self, "错误", f"导入失败:\n{str(e)}")
    
    def import_from_sql(self, file_path):
        """从SQL文件导入"""
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        conn = self.open_databases[self.current_db_path]
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        cursor.executescript(sql_script)
        conn.commit()
    
    def import_from_csv(self, file_path):
        """从CSV文件导入"""
        # 获取文件名作为表名
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        
        # 确认表名
        table_name, ok = QInputDialog.getText(
            self, "表名", "输入要导入的表名:", text=table_name
        )
        
        if not ok or not table_name:
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            data = list(reader)
        
        # 创建表
        conn = self.open_databases[self.current_db_path]
        cursor = conn.cursor()
        cursor.execute("BEGIN")
        
        # 尝试推断列类型
        col_types = []
        if data:
            for i, value in enumerate(data[0]):
                if not value:
                    col_types.append("TEXT")
                    continue
                
                try:
                    int(value)
                    col_types.append("INTEGER")
                except ValueError:
                    try:
                        float(value)
                        col_types.append("REAL")
                    except ValueError:
                        col_types.append("TEXT")
        
        # 构建CREATE TABLE语句
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_sql += ", ".join(f"{header} {col_type}" for header, col_type in zip(headers, col_types))
        create_sql += ")"
        
        cursor.execute(create_sql)
        
        # 插入数据
        insert_sql = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(['?']*len(headers))})"
        
        for row in data:
            # 处理空值
            row = [None if not cell else cell for cell in row]
            cursor.execute(insert_sql, row)
        
        conn.commit()
    
    def toggle_theme(self, checked):
        """切换主题"""
        if checked:
            # 深色主题
            palette = QPalette()
            palette.setColor(QPalette.Window, QColor(53, 53, 53))
            palette.setColor(QPalette.WindowText, Qt.white)
            palette.setColor(QPalette.Base, QColor(25, 25, 25))
            palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
            palette.setColor(QPalette.ToolTipBase, Qt.white)
            palette.setColor(QPalette.ToolTipText, Qt.white)
            palette.setColor(QPalette.Text, Qt.white)
            palette.setColor(QPalette.Button, QColor(53, 53, 53))
            palette.setColor(QPalette.ButtonText, Qt.white)
            palette.setColor(QPalette.BrightText, Qt.red)
            palette.setColor(QPalette.Link, QColor(42, 130, 218))
            palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
            palette.setColor(QPalette.HighlightedText, Qt.black)
            QApplication.setPalette(palette)
        else:
            # 浅色主题
            QApplication.setPalette(QApplication.style().standardPalette())
        
        self.settings.setValue("darkTheme", checked)
    
    def toggle_layout(self, checked):
        """切换布局"""
        if checked:
            # 紧凑布局
            self.content_splitter.setSizes([30, 300, 200])
            self.sql_editor.setFont(QFont("Consolas", 9))
            self.sql_result_text.setFont(QFont("Consolas", 9))
        else:
            # 常规布局
            self.content_splitter.setSizes([50, 500, 350])
            self.sql_editor.setFont(QFont("Consolas", 10))
            self.sql_result_text.setFont(QFont("Consolas", 10))
        
        self.settings.setValue("compactLayout", checked)
    
    def show_help(self):
        """显示帮助"""
        help_text = ProjectInfo.HELP_TEXT
        
        dialog = QDialog(self)
        dialog.setWindowTitle("帮助")
        dialog.resize(600, 500)
        
        layout = QVBoxLayout()
        dialog.setLayout(layout)
        
        text_browser = QTextBrowser()
        text_browser.setPlainText(help_text)
        text_browser.setFont(QFont("Arial", 10))
        
        layout.addWidget(text_browser)
        
        button_box = QDialogButtonBox(QDialogButtonBox.Ok)
        button_box.accepted.connect(dialog.accept)
        layout.addWidget(button_box)
        
        dialog.exec_()
    
    def show_about_dialog(self):
        """显示关于对话框"""
        about_text = f"""
        <h2>{ProjectInfo.NAME}</h2>
        <p>版本: {ProjectInfo.VERSION}</p>
        <p>构建日期: {ProjectInfo.BUILD_DATE}</p>
        <p>作者: {ProjectInfo.AUTHOR}</p>
        <p>许可证: {ProjectInfo.LICENSE}</p>
        <p>版权所有: {ProjectInfo.COPYRIGHT}</p>
        <p>网站: <a href="{ProjectInfo.URL}">{ProjectInfo.URL}</a></p>
        <p>描述: {ProjectInfo.DESCRIPTION}</p>
        """
        
        QMessageBox.about(self, "关于", about_text)
    
    def check_for_updates(self):
        """检查更新"""
        # 这里应该是实际的更新检查逻辑
        # 现在只是模拟
        QMessageBox.information(self, "检查更新", "当前已是最新版本")
    
    def closeEvent(self, event):
        """关闭事件"""
        self.save_settings()
        
        # 关闭所有数据库连接
        for db_path, conn in self.open_databases.items():
            conn.close()
        
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 显示启动画面
    splash_pix = QPixmap("icon.png")  # 替换为你的启动图片
    splash = QSplashScreen(splash_pix, Qt.WindowStaysOnTopHint)
    splash.show()
    
    # 模拟加载过程
    splash.showMessage("正在初始化...", Qt.AlignBottom | Qt.AlignCenter, Qt.white)
    QCoreApplication.processEvents()
    
    # 设置应用程序字体
    font = QFont()
    font.setFamily("Microsoft YaHei")
    font.setPointSize(10)
    app.setFont(font)
    
    # 检查是否通过文件关联打开
    db_path = None
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    # 检查图标文件是否存在，不存在则创建
    icon_path = "icon.ico"
    if not os.path.exists(icon_path):
        from PIL import Image, ImageDraw, ImageFont
        
        try:
            # 创建一个简单的图标
            img = Image.new('RGBA', (256, 256), (70, 130, 180, 255))
            draw = ImageDraw.Draw(img)
            
            try:
                font = ImageFont.truetype("arial.ttf", 120)
            except:
                font = ImageFont.load_default()
                
            draw.text((50, 50), "DB", font=font, fill=(255, 255, 255, 255))
            img.save(icon_path, format='ICO')
        except Exception as e:
            print(f"无法创建图标文件: {e}")
    
    # 创建主窗口
    window = DatabaseManager(db_path)
    
    # 关闭启动画面
    splash.finish(window)
    
    window.show()
    
    sys.exit(app.exec_())