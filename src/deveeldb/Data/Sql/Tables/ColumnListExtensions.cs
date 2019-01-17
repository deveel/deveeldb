using System;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Tables
{
    public static class ColumnListExtensions
    {
        public static IColumnList Add(this IColumnList columns, string columnName, SqlType columnType, SqlExpression defaultValue = null)
        {
            columns.Add(new ColumnInfo(columnName, columnType, defaultValue));
            return columns;
        } 
    }
}