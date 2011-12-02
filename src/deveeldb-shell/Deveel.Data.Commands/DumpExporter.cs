//#if DEBUG
//using System;
//using System.Collections;
//using System.Data;
//using System.IO;
//using System.Text;

//using Deveel.Collections;
//using Deveel.Commands;
//using Deveel.Data.Shell;
//using Deveel.Design;
//using Deveel.Shell;

//namespace Deveel.Data.Commands {
//    internal class DumpExporter {
//        public DumpExporter(DumpCommand command, string fileName) {
//            this.command = command;
//            this.fileName = fileName;
//        }

//        private readonly string fileName;
//        private readonly DumpCommand command;

//        private CommandResultCode dumpTable(SqlSession session, string tabName, string whereClause, TextWriter dumpOut, String fileEncoding) {

//            // asking for meta data is only possible with the correct
//            // table name.
//            bool correctName = true;
//            if (tabName.StartsWith("\"")) {
//                //tabName = stripQuotes(tabName);
//                correctName = false;
//            }

//            // separate schama and table.
//            String schema = null;
//            int schemaDelim = tabName.IndexOf('.');
//            if (schemaDelim > 0) {
//                schema = tabName.Substring(0, schemaDelim);
//                tabName = tabName.Substring(schemaDelim + 1);
//            }

//            if (correctName) {
//                String alternative = session.CorrectTableName(tabName);
//                if (alternative != null && !alternative.Equals(tabName)) {
//                    tabName = alternative;
//                    OutputDevice.Out.WriteLine("dumping table: '" + tabName + "' (corrected name)");
//                }
//            }
//            TableDumpSource tableSource = new TableDumpSource(schema, tabName, !correctName, session);
//            tableSource.SetWhereClause(whereClause);
//            return dumpTable(session, tableSource, dumpOut, fileEncoding);
//        }

//        private CommandResultCode dumpTable(SqlSession session,
//                              IDumpSource dumpSource,
//                              TextWriter dumpOut, String fileEncoding) {
//            DateTime startTime = DateTime.Now;
//            MetaProperty[] metaProps = dumpSource.MetaProperties;
//            if (metaProps.Length == 0) {
//                OutputDevice.Message.WriteLine("No fields in " + dumpSource.Description + " found.");
//                return CommandResultCode.ExecutionFailed;
//            }

//            OutputDevice.Message.WriteLine("dump " + dumpSource.TableName + ":");

//            dumpOut.WriteLine("(tabledump '" + dumpSource.TableName + "'");
//            dumpOut.WriteLine("  (file-encoding '" + fileEncoding + "')");
//            dumpOut.WriteLine("  (dump-version " + DumpCommand.DUMP_VERSION + " " + DumpCommand.DUMP_VERSION + ")");
//            /*
//            if (whereClause != null) {
//                dumpOut.Write("  (where-clause ");
//                QuoteString(dumpOut, whereClause);
//                dumpOut.Write(")");
//            }
//            */
//            dumpOut.WriteLine("  (client-version '" + Deveel.Shell.ProductInfo.Current.Version.ToString(2) + "')");
//            dumpOut.WriteLine("  (time '" + DateTime.Now.ToString() + "')");
//            dumpOut.Write("  (database-info ");
//            quoteString(dumpOut, session.DatabaseInfo);
//            dumpOut.WriteLine(")");

//            long expectedRows = dumpSource.ExpectedRows;
//            dumpOut.WriteLine("  (estimated-rows '" + expectedRows + "')");

//            dumpOut.Write("  (meta (");
//            for (int i = 0; i < metaProps.Length; ++i) {
//                MetaProperty p = metaProps[i];
//                printWidth(dumpOut, p.fieldName, p.RenderWidth, i != 0);
//            }
//            dumpOut.WriteLine(")");
//            dumpOut.Write("\t(");
//            for (int i = 0; i < metaProps.Length; ++i) {
//                MetaProperty p = metaProps[i];
//                printWidth(dumpOut, p.typeName, p.RenderWidth, i != 0);
//            }
//            dumpOut.WriteLine("))");

//            dumpOut.Write("  (data ");
//            IDataReader rset = null;
//            IDbCommand stmt = null;
//            try {
//                long rows = 0;
//                ProgressWriter progressWriter = new ProgressWriter(expectedRows, OutputDevice.Message);
//                rset = dumpSource.Reader;
//                stmt = dumpSource.Command;
//                bool isFirst = true;
//                while (command.IsRunning && rset.Read()) {
//                    ++rows;
//                    progressWriter.Update(rows);
//                    if (!isFirst)
//                        dumpOut.Write("\n\t");
//                    isFirst = false;
//                    dumpOut.Write("(");

//                    for (int i = 0; i < metaProps.Length; ++i) {
//                        DbType thisType = metaProps[i].Type;

//                        switch (thisType) {
//                            case DbType.Numeric:
//                            case DbType.NumericExtended: {
//                                    String val = rset.GetString(i);
//                                    if (rset.IsDBNull(i))
//                                        dumpOut.Write("NULL");
//                                    else
//                                        dumpOut.Write(val);
//                                    break;
//                                }

//                            case DbType.Time: {
//                                    DateTime val = rset.GetDateTime(i);
//                                    if (rset.IsDBNull(i))
//                                        dumpOut.Write("NULL");
//                                    else {
//                                        quoteString(dumpOut, val.ToString());
//                                    }
//                                    break;
//                                }

//                            case DbType.String: {
//                                    String val = rset.GetString(i);
//                                    if (rset.IsDBNull(i))
//                                        dumpOut.Write("NULL");
//                                    else {
//                                        quoteString(dumpOut, val);
//                                    }
//                                    break;
//                                }

//                            default:
//                                throw new ArgumentException("type " + MetaProperty.Types[thisType] + " not supported yet");
//                        }
//                        if (metaProps.Length > i)
//                            dumpOut.Write(",");
//                        else
//                            dumpOut.Write(")");
//                    }
//                }
//                progressWriter.Finish();
//                dumpOut.WriteLine(")");
//                dumpOut.WriteLine("  (rows " + rows + "))\n");

//                OutputDevice.Message.Write("(" + rows + " rows)\n");
//                long execTime = (long)(DateTime.Now - startTime).TotalMilliseconds;

//                OutputDevice.Message.Write("dumping '" + dumpSource.TableName + "' took ");
//                TimeRenderer.PrintTime(execTime, OutputDevice.Message);
//                OutputDevice.Message.Write(" total; ");
//                TimeRenderer.PrintFraction(execTime, rows, OutputDevice.Message);
//                OutputDevice.Message.WriteLine(" / row");
//                if (expectedRows >= 0 && rows != expectedRows) {
//                    OutputDevice.Message.WriteLine(" == Warning: 'select count(*)' in the"
//                                          + " beginning resulted in " + expectedRows
//                                          + " but the dump exported " + rows
//                                          + " rows == ");
//                }

//                if (!command.IsRunning) {
//                    OutputDevice.Message.WriteLine(" == INTERRUPTED. Wait for statement to cancel.. ==");
//                    if (stmt != null)
//                        stmt.Cancel();
//                }
//            } catch (Exception e) {
//                //HenPlus.msg().println(selectStmt.toString());
//                throw e; // handle later.
//            } finally {
//                if (rset != null) {
//                    try {
//                        rset.Close();
//                    } catch (Exception) {

//                    }
//                }
//            }
//            return CommandResultCode.Success;
//        }

//        private static void quoteString(TextWriter output, String input) {
//            StringBuilder buf = new StringBuilder();
//            buf.Append("'");
//            int len = input.Length;
//            for (int i = 0; i < len; ++i) {
//                char c = input[i];
//                if (c == '\'' || c == '\\') {
//                    buf.Append("\\");
//                }
//                buf.Append(c);
//            }
//            buf.Append("'");
//            output.Write(buf.ToString());
//        }

//        // to make the field-name and field-type nicely aligned
//        private void printWidth(TextWriter output, string s, int width, bool comma) {
//            if (comma)
//                output.Write(", ");
//            output.Write("'");
//            output.Write(s);
//            output.Write("'");
//            for (int i = s.Length; i < width; ++i) {
//                output.Write(' ');
//            }
//        }

//        public CommandResultCode Export(SqlSession session, IList possibleTables) {
//            TextWriter output = null;
//            string tabName = null;
//            command.BeginInterruptableSection();
//            try {
//                DateTime startTime = DateTime.Now;
//                ArrayList alreadyDumped = new ArrayList();      // which tables got already dumped?

//                output = command.OpenOutputStream(fileName, "UTF-8");
//                IList/*<String>*/ tableSet = new ArrayList();

//                /* right now, we do only a sort, if there is any '*' found in tables. Probably
//                 * we might want to make this an option to dump-in */
//                bool needsSort = false;

//                CommandResultCode dumpResult = CommandResultCode.Success;

//                /* 1) collect tables */
//                foreach (string nextToken in possibleTables) {
//                    if ("*".Equals(nextToken) || nextToken.IndexOf('*') > -1) {
//                        needsSort = true;

//                        IEnumerator iter = null;

//                        if ("*".Equals(nextToken)) {
//                            iter = session.TableCompleter.GetNamesEnumerator();
//                        } else if (nextToken.IndexOf('*') > -1) {
//                            String tablePrefix = nextToken.Substring(0, nextToken.Length - 1);
//                            ISortedSet tableNames = session.TableCompleter.GetNames();
//                            NameCompleter compl = new NameCompleter(tableNames);
//                            iter = compl.GetAlternatives(tablePrefix);
//                        }
//                        while (iter.MoveNext()) {
//                            tableSet.Add(iter.Current);
//                        }
//                    } else {
//                        tableSet.Add(nextToken);
//                    }
//                }

//                /* 2) resolve dependencies */
//                DependencyResolver.ResolverResult resolverResult = null;
//                IList/*<String>*/ tableSequence;
//                if (needsSort) {
//                    tableSequence = new ArrayList();
//                    OutputDevice.Message.WriteLine("Retrieving and sorting tables. This may take a while, please be patient.");

//                    // get sorted tables
//                    /*
//                    TODO:
//                    SQLMetaData meta = new SQLMetaDataBuilder().getMetaData(session,
//                                                                             tableSet.GetEnumerator());
//                    DependencyResolver dr = new DependencyResolver(meta.getTables());
//                    resolverResult = dr.sortTables();
//                    IList/*<Table> tabs = resolverResult.getTables();
//                    foreach (Shell.Table table in tabs) {
//                        tableSequence.Add(table.Name);
//                    }
//                    */
//                } else {
//                    tableSequence = new ArrayList(tableSet);
//                }

//                /* 3) dump out */
//                if (tableSequence.Count > 1) {
//                    OutputDevice.Message.WriteLine(tableSequence.Count + " tables to dump.");
//                }
//                IEnumerator it = tableSequence.GetEnumerator();
//                while (command.IsRunning && it.MoveNext()) {
//                    string table = (String)it.Current;
//                    if (!alreadyDumped.Contains(table)) {
//                        // TODO: CommandResultCode result = dumpTable(session, table, null, output, "UTF-8", alreadyDumped);
//                        CommandResultCode result = CommandResultCode.ExecutionFailed;
//                        if (result != CommandResultCode.Success) {
//                            dumpResult = result;
//                        }
//                    }
//                }

//                if (tableSequence.Count > 1) {
//                    long duration = (long)(DateTime.Now - startTime).TotalMilliseconds;
//                    OutputDevice.Message.WriteLine("Dumping " + tableSequence.Count + " tables took ");
//                    TimeRenderer.PrintTime(duration, OutputDevice.Message);
//                    OutputDevice.Message.WriteLine();
//                }

//                /* 4) warn about cycles */
//                if (resolverResult != null
//                     && resolverResult.getCyclicDependencies() != null
//                     && resolverResult.getCyclicDependencies().Count > 0) {
//                    OutputDevice.Message.WriteLine("-----------\n"
//                                           + "NOTE: There have been cyclic dependencies between several tables detected.\n" +
//                                           "These may cause trouble when dumping in the currently dumped data.");
//                    IEnumerator iter = resolverResult.getCyclicDependencies().GetEnumerator();
//                    int count = 0;
//                    StringBuilder sb = new StringBuilder();
//                    while (iter.MoveNext()) {
//                        IEnumerator iter2 = ((IList)iter.Current).GetEnumerator();
//                        sb.Append("Cycle ").Append(count).Append(": ");
//                        while (iter2.MoveNext()) {
//                            sb.Append(((Shell.Table)iter2.Current).Name).Append(" -> ");
//                        }
//                        sb.Remove(sb.Length - 4, 4).Append('\n');
//                    }
//                    OutputDevice.Message.Write(sb.ToString());
//                    /* todo: print out, what constraint to disable */
//                }

//                return dumpResult;
//            } catch (Exception e) {
//                OutputDevice.Message.WriteLine("dump table '" + tabName + "' failed: " + e.Message);
//                OutputDevice.Message.WriteLine(e.StackTrace);
//                return CommandResultCode.ExecutionFailed;
//            } finally {
//                if (output != null) output.Close();
//                command.EndInterruptableSection();
//            }
//        }
//    }
//}
//#endif