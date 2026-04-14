#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信创数据库迁移工具 (MVP版本)
目标：将MySQL数据迁移到GaussDB(兼容PostgreSQL语法)
"""

import streamlit as st
import pymysql
import psycopg2
import pandas as pd
import time
from typing import Dict, List, Tuple

# 数据类型映射：MySQL -> GaussDB
TYPE_MAPPING = {
    'datetime': 'timestamp',
    'int': 'integer',
    'bigint': 'bigint',
    'float': 'float',
    'double': 'double precision',
    'varchar': 'varchar',
    'char': 'char',
    'text': 'text',
    'longtext': 'text',
    'boolean': 'boolean',
    'date': 'date',
    'time': 'time'
}

# 批量迁移的批次大小
BATCH_SIZE = 1000


def test_mysql_connection(host: str, port: int, user: str, password: str, db: str) -> Tuple[bool, str]:
    """
    测试MySQL数据库连接
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
    
    Returns:
        (是否连接成功, 错误信息或成功信息)
    """
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        conn.close()
        return True, "MySQL连接成功！"
    except Exception as e:
        return False, f"MySQL连接失败: {str(e)}"


def test_gaussdb_connection(host: str, port: int, user: str, password: str, db: str) -> Tuple[bool, str]:
    """
    测试GaussDB数据库连接
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
    
    Returns:
        (是否连接成功, 错误信息或成功信息)
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db
        )
        conn.close()
        return True, "GaussDB连接成功！"
    except Exception as e:
        return False, f"GaussDB连接失败: {str(e)}"


def get_mysql_tables(host: str, port: int, user: str, password: str, db: str) -> List[str]:
    """
    获取MySQL数据库中的所有表名
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
    
    Returns:
        表名列表
    """
    tables = []
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            results = cursor.fetchall()
            for row in results:
                tables.append(list(row.values())[0])
        conn.close()
    except Exception as e:
        st.error(f"获取MySQL表名失败: {str(e)}")
    return tables


def get_mysql_table_structure(host: str, port: int, user: str, password: str, db: str, table: str) -> List[Dict]:
    """
    获取MySQL表结构
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
        table: 表名
    
    Returns:
        表结构列表，每个元素包含字段名、数据类型、是否可为空、默认值等信息
    """
    structure = []
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table}")
            results = cursor.fetchall()
            for row in results:
                field_info = {
                    'Field': row['Field'],
                    'Type': row['Type'],
                    'Null': row['Null'],
                    'Default': row['Default'],
                    'Extra': row['Extra']
                }
                structure.append(field_info)
        conn.close()
    except Exception as e:
        st.error(f"获取表结构失败: {str(e)}")
    return structure


def create_gaussdb_table(host: str, port: int, user: str, password: str, db: str, table: str, structure: List[Dict]) -> bool:
    """
    在GaussDB中创建表
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
        table: 表名
        structure: 表结构
    
    Returns:
        是否创建成功
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db
        )
        
        # 构建CREATE TABLE语句
        columns = []
        for field in structure:
            # 提取数据类型
            mysql_type = field['Type']
            # 提取基本类型（去掉长度等信息）
            base_type = mysql_type.split('(')[0].lower()
            # 特殊处理tinyint(1)，转换为boolean
            if base_type == 'tinyint' and '(1)' in mysql_type:
                gauss_type = 'boolean'
            else:
                # 转换为GaussDB类型
                gauss_type = TYPE_MAPPING.get(base_type, base_type)
                # 保留长度信息（如果有）
                if '(' in mysql_type:
                    gauss_type += mysql_type[mysql_type.find('('):]
            
            # 构建列定义
            column_def = f"{field['Field']} {gauss_type}"
            if field['Null'] == 'NO':
                column_def += " NOT NULL"
            if field['Default'] is not None:
                if field['Default'] == 'CURRENT_TIMESTAMP':
                    column_def += " DEFAULT CURRENT_TIMESTAMP"
                else:
                    column_def += f" DEFAULT '{field['Default']}'"
            if 'auto_increment' in field['Extra'].lower():
                column_def += " PRIMARY KEY"
            
            columns.append(column_def)
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table} (" + ", ".join(columns) + ")"
        
        with conn.cursor() as cursor:
            cursor.execute(create_sql)
            conn.commit()
        
        conn.close()
        return True
    except Exception as e:
        st.error(f"创建GaussDB表失败: {str(e)}")
        return False


def migrate_data(host_mysql: str, port_mysql: int, user_mysql: str, password_mysql: str, db_mysql: str,
                 host_gauss: str, port_gauss: int, user_gauss: str, password_gauss: str, db_gauss: str,
                 table: str, progress_bar, status_text) -> int:
    """
    迁移数据
    
    Args:
        host_mysql: MySQL主机地址
        port_mysql: MySQL端口号
        user_mysql: MySQL用户名
        password_mysql: MySQL密码
        db_mysql: MySQL数据库名
        host_gauss: GaussDB主机地址
        port_gauss: GaussDB端口号
        user_gauss: GaussDB用户名
        password_gauss: GaussDB密码
        db_gauss: GaussDB数据库名
        table: 表名
        progress_bar: 进度条对象
        status_text: 状态文本对象
    
    Returns:
        迁移的记录数
    """
    total_rows = 0
    try:
        # 连接MySQL
        conn_mysql = pymysql.connect(
            host=host_mysql,
            port=port_mysql,
            user=user_mysql,
            password=password_mysql,
            database=db_mysql,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        
        # 连接GaussDB
        conn_gauss = psycopg2.connect(
            host=host_gauss,
            port=port_gauss,
            user=user_gauss,
            password=password_gauss,
            dbname=db_gauss
        )
        
        # 获取表结构，用于构建插入语句
        structure = get_mysql_table_structure(host_mysql, port_mysql, user_mysql, password_mysql, db_mysql, table)
        fields = [field['Field'] for field in structure]
        fields_str = ", ".join(fields)
        placeholders = ", ".join(["%s"] * len(fields))
        insert_sql = f"INSERT INTO {table} ({fields_str}) VALUES ({placeholders})"
        
        # 获取总记录数
        with conn_mysql.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cursor.fetchone()['COUNT(*)']
        
        # 批量迁移数据
        migrated_rows = 0
        with conn_mysql.cursor() as cursor_mysql, conn_gauss.cursor() as cursor_gauss:
            # 流式读取数据
            cursor_mysql.execute(f"SELECT * FROM {table}")
            
            while True:
                rows = cursor_mysql.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                
                # 准备插入数据
                data = []
                for row in rows:
                    row_data = [row[field] for field in fields]
                    data.append(row_data)
                
                # 批量插入
                cursor_gauss.executemany(insert_sql, data)
                conn_gauss.commit()
                
                migrated_rows += len(rows)
                
                # 更新进度
                progress = migrated_rows / total_rows if total_rows > 0 else 1.0
                progress_bar.progress(progress)
                status_text.text(f"正在迁移 {table}: {migrated_rows}/{total_rows} 条记录")
                
                # 短暂暂停，避免过快的API调用
                time.sleep(0.1)
        
        conn_mysql.close()
        conn_gauss.close()
        
    except Exception as e:
        st.error(f"数据迁移失败: {str(e)}")
    
    return total_rows


def get_table_row_count(host: str, port: int, user: str, password: str, db: str, table: str, is_mysql: bool) -> int:
    """
    获取表的记录数
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
        table: 表名
        is_mysql: 是否为MySQL数据库
    
    Returns:
        记录数
    """
    count = 0
    try:
        if is_mysql:
            conn = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()['COUNT(*)']
        else:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=db
            )
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
        conn.close()
    except Exception as e:
        st.error(f"获取表记录数失败: {str(e)}")
    return count


def get_mysql_version(host: str, port: int, user: str, password: str, db: str) -> str:
    """
    获取MySQL数据库版本
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
    
    Returns:
        版本号字符串
    """
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()['VERSION()']
        conn.close()
        return version
    except Exception as e:
        st.error(f"获取MySQL版本失败: {str(e)}")
        return "未知"


def get_gaussdb_version(host: str, port: int, user: str, password: str, db: str) -> str:
    """
    获取GaussDB数据库版本
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
    
    Returns:
        版本号字符串
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db
        )
        with conn.cursor() as cursor:
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
        conn.close()
        return version
    except Exception as e:
        st.error(f"获取GaussDB版本失败: {str(e)}")
        return "未知"


def get_total_row_count(host: str, port: int, user: str, password: str, db: str, tables: List[str], is_mysql: bool) -> int:
    """
    统计所有表的总行数
    
    Args:
        host: 主机地址
        port: 端口号
        user: 用户名
        password: 密码
        db: 数据库名
        tables: 表名列表
        is_mysql: 是否为MySQL数据库
    
    Returns:
        总行数
    """
    total_count = 0
    for table in tables:
        total_count += get_table_row_count(host, port, user, password, db, table, is_mysql)
    return total_count


def main():
    """
    主函数
    """
    # 设置页面标题
    st.title("信创数据库迁移工具 (MVP版本)")
    st.markdown("将MySQL数据迁移到GaussDB(兼容PostgreSQL语法)")
    
    # 创建两个列，分别用于源数据库和目标数据库
    col1, col2 = st.columns(2)
    
    # 源数据库（MySQL）配置
    with col1:
        st.subheader("源数据库 (MySQL)")
        mysql_host = st.text_input("Host", "localhost", key="mysql_host")
        mysql_port = st.number_input("Port", 1, 65535, 3306, key="mysql_port")
        mysql_user = st.text_input("User", "root", key="mysql_user")
        mysql_password = st.text_input("Password", "", type="password", key="mysql_password")
        mysql_db = st.text_input("Database", "test", key="mysql_db")
        test_mysql_btn = st.button("测试MySQL连接")
    
    # 目标数据库（GaussDB）配置
    with col2:
        st.subheader("目标数据库 (GaussDB)")
        gauss_host = st.text_input("Host", "localhost", key="gauss_host")
        gauss_port = st.number_input("Port", 1, 65535, 5432, key="gauss_port")
        gauss_user = st.text_input("User", "postgres", key="gauss_user")
        gauss_password = st.text_input("Password", "", type="password", key="gauss_password")
        gauss_db = st.text_input("Database", "test", key="gauss_db")
        test_gauss_btn = st.button("测试GaussDB连接")
    
    # 测试MySQL连接
    if test_mysql_btn:
        success, message = test_mysql_connection(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db)
        if success:
            st.success(message)
        else:
            st.error(message)
    
    # 测试GaussDB连接
    if test_gauss_btn:
        success, message = test_gaussdb_connection(gauss_host, gauss_port, gauss_user, gauss_password, gauss_db)
        if success:
            st.success(message)
        else:
            st.error(message)
    
    # 数据类型转换规则
    with st.expander("数据类型转换规则"):
        st.markdown("""
        | MySQL类型 | GaussDB类型 | 说明 |
        |----------|------------|------|
        | datetime | timestamp | 日期时间类型 |
        | int | integer | 整数类型 |
        | bigint | bigint | 大整数类型 |
        | float | float | 浮点数类型 |
        | double | double precision | 双精度浮点数 |
        | varchar | varchar | 可变长度字符串 |
        | char | char | 固定长度字符串 |
        | text | text | 文本类型 |
        | longtext | text | 长文本类型 |
        | boolean | boolean | 布尔类型 |
        | tinyint(1) | boolean | 布尔类型（MySQL中常用tinyint(1)表示布尔值） |
        | date | date | 日期类型 |
        | time | time | 时间类型 |
        """)
    
    # 迁移按钮
    migrate_btn = st.button("开始迁移")
    
    if migrate_btn:
        # 测试连接
        mysql_ok, _ = test_mysql_connection(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db)
        gauss_ok, _ = test_gaussdb_connection(gauss_host, gauss_port, gauss_user, gauss_password, gauss_db)
        
        if not mysql_ok or not gauss_ok:
            st.error("请先确保两个数据库连接正常！")
            return
        
        # 开始迁移前校验
        st.info("开始迁移前校验...")
        
        # 获取源库表名
        tables = get_mysql_tables(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db)
        if not tables:
            st.error("未找到源数据库中的表！")
            return
        
        # 获取数据库版本
        mysql_version = get_mysql_version(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db)
        gauss_version = get_gaussdb_version(gauss_host, gauss_port, gauss_user, gauss_password, gauss_db)
        
        # 统计总行数
        total_rows = get_total_row_count(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db, tables, True)
        
        # 预估迁移时间（假设每1000条记录需要1秒）
        estimated_time = total_rows / 1000
        minutes = int(estimated_time // 60)
        seconds = int(estimated_time % 60)
        
        # 显示校验结果
        st.markdown(f"""
        ### 迁移前校验结果
        - **MySQL版本**: {mysql_version}
        - **GaussDB版本**: {gauss_version}
        - **要迁移的表数**: {len(tables)}
        - **要迁移的总行数**: {total_rows}
        - **预估迁移时间**: {minutes}分{seconds}秒
        """)
        
        # 确认迁移
        if not st.button("确认开始迁移"):
            st.info("迁移已取消")
            return
        
        # 开始迁移
        st.info("开始迁移数据...")
        
        # 创建进度条和状态文本
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 迁移结果
        migration_results = []
        
        # 开始迁移
        total_tables = len(tables)
        for i, table in enumerate(tables):
            # 更新状态
            status_text.text(f"正在处理表: {table} ({i+1}/{total_tables})")
            
            # 获取表结构
            structure = get_mysql_table_structure(mysql_host, mysql_port, mysql_user, mysql_password, mysql_db, table)
            
            # 在目标库创建表
            create_success = create_gaussdb_table(gauss_host, gauss_port, gauss_user, gauss_password, gauss_db, table, structure)
            
            if create_success:
                # 迁移数据
                source_count = migrate_data(
                    mysql_host, mysql_port, mysql_user, mysql_password, mysql_db,
                    gauss_host, gauss_port, gauss_user, gauss_password, gauss_db,
                    table, progress_bar, status_text
                )
                
                # 获取目标库数据量
                target_count = get_table_row_count(gauss_host, gauss_port, gauss_user, gauss_password, gauss_db, table, False)
                
                # 记录结果
                migration_results.append({
                    '表名': table,
                    '源库数据量': source_count,
                    '目标库数据量': target_count,
                    '状态': '成功' if source_count == target_count else '失败'
                })
            else:
                migration_results.append({
                    '表名': table,
                    '源库数据量': 0,
                    '目标库数据量': 0,
                    '状态': '创建表失败'
                })
            
            # 更新总体进度
            overall_progress = (i + 1) / total_tables
            progress_bar.progress(overall_progress)
        
        # 完成迁移
        progress_bar.progress(1.0)
        status_text.text("迁移完成！")
        
        # 显示迁移结果
        st.subheader("迁移结果")
        if migration_results:
            df = pd.DataFrame(migration_results)
            st.dataframe(df)
            
            # 计算总体统计
            total_source = sum(item['源库数据量'] for item in migration_results)
            total_target = sum(item['目标库数据量'] for item in migration_results)
            success_count = sum(1 for item in migration_results if item['状态'] == '成功')
            
            st.markdown(f"""
            ### 迁移统计
            - 总表数: {total_tables}
            - 成功迁移表数: {success_count}
            - 源库总数据量: {total_source}
            - 目标库总数据量: {total_target}
            - 迁移状态: {'成功' if total_source == total_target else '失败'}
            """)
        else:
            st.info("没有表需要迁移")


if __name__ == "__main__":
    main()