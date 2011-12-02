//#if DEBUG
//using System;
//using System.Data;

//using Deveel.Data.Shell;

//namespace Deveel.Data.Commands {
//    class SelectDumpSource : IDumpSource {
//        private readonly SqlSession session;
//        private readonly string sqlStatement;
//        private readonly string exportTable;
//        private MetaProperty[] meta;
//        private IDbCommand workingCommand;
//        private IDataReader reader;

//        SelectDumpSource(SqlSession session, string exportTable, string sqlStatement) {
//            this.session = session;
//            this.sqlStatement = sqlStatement;
//            this.exportTable = exportTable;
//        }

//        public MetaProperty[] MetaProperties {
//            get {
//                if (meta != null)
//                    return meta;

//                IDataReader rset = Reader;

//                System.Data.DataTable schemaTable = rset.GetSchemaTable();
//                int cols = schemaTable.Columns.Count;
//                meta = new MetaProperty[cols];
//                for (int i = 0; i < cols; ++i) {
//                    string columnName = schemaTable.Rows[i]["Name"].ToString();
//                    SqlType sqlTypes = (SqlType)schemaTable.Rows[i]["Type"];
//                    meta[i] = new MetaProperty(columnName, sqlTypes);
//                }
//                return meta;
//            }
//        }

//        public String Description {
//            get { return sqlStatement; }
//        }

//        public String TableName {
//            get { return exportTable; }
//        }

//        public IDbCommand Command {
//            get { return workingCommand; }
//        }

//        public IDataReader Reader {
//            get {
//                if (reader != null) {
//                    return reader;
//                }
//                workingCommand = session.CreateCommand();
//                workingCommand.CommandText = sqlStatement;
//                reader = workingCommand.ExecuteReader();
//                return reader;
//            }
//        }

//        public long ExpectedRows {
//            get { return -1; }
//        }
//    }
//}
//#endif