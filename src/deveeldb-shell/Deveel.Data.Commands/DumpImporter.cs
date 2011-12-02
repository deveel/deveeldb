//#if DEBUG
//using System;
//using System.Collections;
//using System.Data;
//using System.IO;
//using System.Text;

//using Deveel.Commands;
//using Deveel.Data.Client;
//using Deveel.Data.Shell;
//using Deveel.Design;
//using Deveel.Math;
//using Deveel.Shell;

//namespace Deveel.Data.Commands {
//    internal class DumpImporter {
//        public DumpImporter(DumpCommand command, string fileName) {
//            this.command = command;
//            this.fileName = fileName;
//        }

//        static DumpImporter() {
//            MetaHeaders = new ColumnDesign[3];
//            MetaHeaders[0] = new ColumnDesign("Field");
//            MetaHeaders[1] = new ColumnDesign("Type");
//            MetaHeaders[2] = new ColumnDesign("Max. length found", ColumnAlignment.Right);
//        }

//        private readonly DumpCommand command;
//        private readonly string fileName;
//        private TextReader reader;
//        private int lineNumber;
//        private string fileEncoding;
//        private int commitPoint;

//        private const string NULL_STR = "NULL";

//        protected static readonly ColumnDesign[] MetaHeaders;


//        public int CommitPoint {
//            get { return commitPoint; }
//            set { commitPoint = value; }
//        }

//        private void RaiseException(string message) {
//            throw new IOException("line " + lineNumber + ": " + message);
//        }

//        private string ReadToken() {
//            SkipWhite();
//            StringBuilder token = new StringBuilder();
//            int c;
//            while ((c = reader.Peek()) > 0) {
//                char ch = (char)c;
//                if (Char.IsWhiteSpace(ch) ||
//                    ch == ';' || ch == ',' ||
//                    ch == '(' || ch == ')') {
//                    break;
//                }
//                token.Append(ch);
//                reader.Read();
//            }
//            return token.ToString();
//        }

//        private bool SkipWhite() {
//            int c;
//            while ((c = reader.Peek()) > 0) {
//                if (c == '\n')
//                    lineNumber++;
//                if (!Char.IsWhiteSpace((char)c))
//                    return true;
//                reader.Read();
//            }
//            return false;
//        }

//        /**
//     * read a string. This is either NULL without quotes or a quoted
//     * string.
//     */
//        private string ReadString() {
//            int nullParseState = 0;
//            int c;
//            while ((c = reader.Read()) > 0) {
//                char ch = (char)c;
//                // unless we already parse the NULL string, skip whitespaces.
//                if (nullParseState == 0 && Char.IsWhiteSpace(ch))
//                    continue;
//                if (ch == '\'')
//                    break; // -> opening string.
//                if (ch == '\n') {
//                    lineNumber++;
//                    continue;
//                }
//                if (Char.ToUpper(ch) == NULL_STR[nullParseState]) {
//                    ++nullParseState;
//                    if (nullParseState == NULL_STR.Length)
//                        return null;
//                    continue;
//                }
//                RaiseException("unecpected character '" + ch + "'");
//            }

//            // ok, we found an opening quote.
//            StringBuilder result = new StringBuilder();
//            while ((c = reader.Read()) > 0) {
//                if (c == '\\') {
//                    c = reader.Read();
//                    if (c < 0) {
//                        RaiseException("excpected character after backslash escape");
//                    }
//                    result.Append((char)c);
//                    continue;
//                }
//                char ch = (char)c;
//                if (ch == '\n')
//                    lineNumber++;
//                if (ch == '\'')
//                    break; // End Of String.

//                result.Append((char)c);
//            }

//            return result.ToString();
//        }


//        private double ReadNumber() {
//            String token = ReadToken();
//            // separated sign.
//            if (token.Length == 1 &&
//                (token.Equals("+") || token.Equals("-"))) {
//                token += ReadToken();
//            }
//            if (token.Equals(NULL_STR))
//                return Double.NaN;
//            try {
//                if (token.IndexOf('.') > 0) {
//                    return Double.Parse(token);
//                }
//                if (token.Length < 10)
//                    return Int32.Parse(token);
//                if (token.Length < 19)
//                    return Int64.Parse(token);
//            } catch (FormatException e) {
//                RaiseException("Number format " + token + ": " + e.Message);
//            }
//            return Double.NaN;
//        }

//        private void Expect(char ch) {
//            SkipWhite();
//            char inCh = (char)reader.Read();
//            if (ch != inCh)
//                RaiseException("'" + ch + "' expected");
//        }

//        protected void checkSupported(int version) {
//            if (version <= 0 || version > DumpCommand.DUMP_VERSION) {
//                throw new ArgumentException("incompatible dump-version");
//            }
//        }

//        private static void PrintMetaDataInfo(MetaProperty[] prop) {
//            OutputDevice.Out.WriteLine();
//            MetaHeaders[0].ResetWidth();
//            MetaHeaders[1].ResetWidth();
//            TableRenderer table = new TableRenderer(MetaHeaders, OutputDevice.Out);
//            for (int i = 0; i < prop.Length; ++i) {
//                ColumnValue[] row = new ColumnValue[3];
//                row[0] = new ColumnValue(prop[i].FieldName);
//                row[1] = new ColumnValue(prop[i].TypeName);
//                row[2] = new ColumnValue(prop[i].MaxLength);
//                table.AddRow(row);
//            }
//            table.CloseTable();
//        }

//        protected MetaProperty[] parseMetaData() {
//            ArrayList metaList = new ArrayList();
//            Expect('(');
//            for (; ; ) {
//                String colName = ReadString();
//                metaList.Add(new MetaProperty(colName));
//                SkipWhite();
//                char inCh = (char)reader.Read();
//                if (inCh == ')')
//                    break;
//                if (inCh != ',') {
//                    RaiseException("',' or ')' expected");
//                }
//            }
//            Expect('(');
//            MetaProperty[] result = (MetaProperty[])metaList.ToArray(typeof(MetaProperty));
//            for (int i = 0; i < result.Length; ++i) {
//                String typeName = ReadString();
//                result[i].TypeName = typeName;
//                Expect((i + 1 < result.Length) ? ',' : ')');
//            }
//            Expect(')');
//            return result;
//        }

//        private DeveelDbParameter CreateParameter(MetaProperty metaProperty) {
//            DbType type = metaProperty.type;

//            DeveelDbParameter parameter = new DeveelDbParameter();

//            switch (type) {
//                case DbType.Numeric:
//                case DbType.NumericExtended: {
//                    double number = ReadNumber();
//                    if (type == DbType.Numeric) {
//                        parameter.SqlType = SqlType.Numeric;
//                    } else if (type == DbType.NumericExtended) {
//                        parameter.SqlType = SqlType.Double;
//                    }

//                    if (number == Double.NaN) {
//                        parameter.Value = null;
//                    } else {
//                        if (type == DbType.Numeric) {
//                            parameter.Value = (BigNumber)number;
//                        } else if (type == DbType.NumericExtended) {
//                            parameter.Value = number;
//                        }
//                    }
//                    break;
//                }

//                case DbType.Time: {
//                    String val = ReadString();
//                    metaProperty.UpdateMaxLength(val);
//                    parameter.SqlType = SqlType.TimeStamp;

//                    if (val == null) {
//                        parameter.Value = null;
//                    } else {
//                        parameter.Value = DateTime.Parse(val);
//                    }
//                    break;
//                }

//                case DbType.String: {
//                    String val = ReadString();
//                    metaProperty.UpdateMaxLength(val);
//                    parameter.SqlType = SqlType.VarChar;
//                    parameter.Value = val;
//                    break;
//                }

//                default:
//                    throw new ArgumentException("type " + MetaProperty.Types[metaProperty.type] + " not supported yet");
//            }

//            return parameter;
//        }

//        public CommandResultCode Import(SqlSession session) {
//            bool hot = (session != null);
//            command.BeginInterruptableSection();
//            try {
//                bool retryPossible = true;
//                do {
//                    try {
//                        reader = command.OpenInputReader(fileName, fileEncoding);
//                        while (SkipWhite()) {
//                            CommandResultCode result = ReadTableDump(session, hot);
//                            retryPossible = false;
//                            if (!command.IsRunning) {
//                                OutputDevice.Message.WriteLine("interrupted.");
//                                return result;
//                            }
//                            if (result != CommandResultCode.Success) {
//                                return result;
//                            }
//                        }
//                    } catch (EncodingMismatchException e) {
//                        // did we already retry with another encoding?
//                        if (!fileEncoding.Equals(command.FileEncoding)) {
//                            throw new Exception("got file encoding problem twice");
//                        }
//                        fileEncoding = e.Encoding;
//                        OutputDevice.Message.WriteLine("got a different encoding; retry with " + fileEncoding);
//                    }
//                } while (retryPossible);

//                return CommandResultCode.Success;
//            } catch (Exception e) {
//                OutputDevice.Message.WriteLine("failed: " + e.Message);
//                OutputDevice.Message.WriteLine(e.StackTrace);
//                return CommandResultCode.ExecutionFailed;
//            } finally {
//                try {
//                    if (reader != null)
//                        reader.Close();
//                } catch (IOException) {
//                    OutputDevice.Message.WriteLine("closing file failed.");
//                }
//                command.EndInterruptableSection();
//            }
//        }

//        private CommandResultCode ReadTableDump(SqlSession session, bool hot) {
//            MetaProperty[] metaProperty = null;
//            string tableName;
//            int dumpVersion = -1;
//            int compatibleVersion = -1;
//            string henplusVersion = null;
//            string databaseInfo = null;
//            string dumpTime = null;
//            string whereClause = null;
//            string token;
//            long importedRows = -1;
//            long expectedRows = -1;
//            long estimatedRows = -1;
//            long problemRows = -1;
//            DeveelDbConnection conn;
//            DeveelDbCommand dbCommand = null;

//            Expect('(');
//            token = ReadToken();
//            if (!"tabledump".Equals(token))
//                RaiseException("'tabledump' expected");

//            tableName = ReadString();
//            DateTime startTime = DateTime.Now;
//            while (command.IsRunning) {
//                SkipWhite();
//                int rawChar = reader.Read();
//                if (rawChar == -1)
//                    return CommandResultCode.Success; // EOF reached.

//                char inCh = (char)rawChar;
//                if (inCh == ')')
//                    break;
//                if (inCh != '(')
//                    RaiseException("'(' or ')' expected");

//                token = ReadToken();

//                if ("dump-version".Equals(token)) {
//                    token = ReadToken();
//                    try {
//                        dumpVersion = Int32.Parse(token);
//                    } catch (Exception) {
//                        RaiseException("expected dump version number");
//                    }

//                    token = ReadToken();
//                    try {
//                        compatibleVersion = Int32.Parse(token);
//                    } catch (Exception) {
//                        RaiseException("expected compatible version number");
//                    }

//                    checkSupported(compatibleVersion);
//                    Expect(')');
//                } else if ("file-encoding".Equals(token)) {
//                    token = ReadString();
//                    if (!token.Equals(fileEncoding)) {
//                        throw new EncodingMismatchException(token);
//                    }
//                    Expect(')');
//                } else if ("client-version".Equals(token)) {
//                    token = ReadString();
//                    henplusVersion = token;
//                    Expect(')');
//                } else if ("rows".Equals(token)) {
//                    token = ReadToken();
//                    expectedRows = Int32.Parse(token);
//                    Expect(')');
//                } else if ("estimated-rows".Equals(token)) {
//                    token = ReadString();
//                    estimatedRows = Int32.Parse(token);
//                    Expect(')');
//                } else if ("database-info".Equals(token)) {
//                    databaseInfo = ReadString();
//                    Expect(')');
//                } else if ("where-clause".Equals(token)) {
//                    whereClause = ReadString();
//                    Expect(')');
//                } else if ("time".Equals(token)) {
//                    dumpTime = ReadString();
//                    Expect(')');
//                } else if ("meta".Equals(token)) {
//                    if (dumpVersion < 0 || compatibleVersion < 0) {
//                        RaiseException("cannot read meta data without dump-version information");
//                    }
//                    metaProperty = parseMetaData();
//                } else if ("data".Equals(token)) {
//                    if (metaProperty == null) {
//                        RaiseException("no meta-data available");
//                    }
//                    if (tableName == null) {
//                        RaiseException("no table name known");
//                    }
//                    if (hot) {
//                        StringBuilder prep = new StringBuilder("INSERT INTO ");
//                        prep.Append(tableName);
//                        prep.Append(" (");
//                        for (int i = 0; i < metaProperty.Length; ++i) {
//                            prep.Append(metaProperty[i].fieldName);
//                            if (i + 1 < metaProperty.Length)
//                                prep.Append(",");
//                        }
//                        prep.Append(") VALUES (");
//                        for (int i = 0; i < metaProperty.Length; ++i) {
//                            prep.Append("?");
//                            if (i + 1 < metaProperty.Length)
//                                prep.Append(",");
//                        }
//                        prep.Append(")");
//                        //HenPlus.msg().println(prep.toString());
//                        conn = session.Connection;
//                        dbCommand = conn.CreateCommand();
//                        dbCommand.CommandText = prep.ToString();
//                    }

//                    OutputDevice.Message.WriteLine((hot ? "importing" : "verifying")
//                                                        + " table dump created with DeveelDB Client "
//                                                        + henplusVersion
//                                                        + "\nfor table           : " + tableName
//                                                        + "\nfrom database       : " + databaseInfo
//                                                        + "\nat                  : " + dumpTime
//                                                        + "\ndump format version : " + dumpVersion);
//                    if (whereClause != null) {
//                        OutputDevice.Message.WriteLine("projection          : " + whereClause);
//                    }

//                    ProgressWriter progressWriter = new ProgressWriter(estimatedRows, OutputDevice.Message);
//                    importedRows = 0;
//                    problemRows = 0;
//                    command.IsRunning = true;
//                    while (command.IsRunning) {
//                        SkipWhite();
//                        inCh = (char)reader.Read();
//                        if (inCh == ')')
//                            break;

//                        if (inCh != '(')
//                            RaiseException("'(' or ')' expected");

//                        // we are now at the beginning of the row.
//                        ++importedRows;
//                        progressWriter.Update(importedRows);

//                        for (int i = 0; i < metaProperty.Length; ++i) {
//                            DeveelDbParameter parameter = CreateParameter(metaProperty[i]);
//                            Expect((i + 1 < metaProperty.Length) ? ',' : ')');

//                            if (dbCommand != null)
//                                dbCommand.Parameters.Add(parameter);
//                        }

//                        try {
//                            if (dbCommand != null) {
//                                dbCommand.Prepare();
//                                dbCommand.ExecuteNonQuery();
//                            }
//                        } catch (DataException e) {
//                            String msg = e.Message;
//                            // oracle adds CR for some reason.
//                            if (msg != null)
//                                msg = msg.Trim();

//                            command.ReportProblem(msg);
//                            ++problemRows;
//                        }

//                        // commit every once in a while.
//                        if (hot && (commitPoint >= 0) && importedRows % commitPoint == 0) {
//                            session.Commit();
//                        }
//                    }
//                    progressWriter.Finish();
//                } else {
//                    OutputDevice.Message.WriteLine("ignoring unknown token " + token);
//                    dumpTime = ReadString();
//                    Expect(')');
//                }
//            }

//            // return final count.
//            command.FinishProblemReports();

//            if (!hot) {
//                PrintMetaDataInfo(metaProperty);
//            }

//            // final commit, if commitPoints are enabled.
//            if (hot && commitPoint >= 0) {
//                session.Commit();
//            }

//            if (expectedRows >= 0 && expectedRows != importedRows) {
//                OutputDevice.Message.WriteLine("WARNING: expected " + expectedRows + " but got " + importedRows + " rows");
//            } else {
//                OutputDevice.Message.WriteLine("ok. ");
//            }

//            OutputDevice.Message.Write("(" + importedRows + " rows total");
//            if (hot)
//                OutputDevice.Message.Write(" / " + problemRows + " with errors");

//            OutputDevice.Message.Write("; ");
//            long execTime = (long)(DateTime.Now - startTime).TotalMilliseconds;
//            TimeRenderer.PrintTime(execTime, OutputDevice.Message);
//            OutputDevice.Message.Write(" total; ");
//            TimeRenderer.PrintFraction(execTime, importedRows, OutputDevice.Message);
//            OutputDevice.Message.WriteLine(" / row)");
//            return CommandResultCode.Success;
//        }
//    }
//}
//#endif