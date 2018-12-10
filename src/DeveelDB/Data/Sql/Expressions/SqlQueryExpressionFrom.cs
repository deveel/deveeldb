// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Query;

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlQueryExpressionFrom : ISqlFormattable, ISqlExpressionPreparable<SqlQueryExpressionFrom> {
		private int sourceKey = -1;
		private List<string> aliases;
		private List<SqlQueryExpressionSource> sources;
		private List<JoinPart> joinParts;

		public SqlQueryExpressionFrom() {
			sources = new List<SqlQueryExpressionSource>();
			aliases = new List<string>();
			joinParts = new List<JoinPart>();
		}

		public IEnumerable<SqlQueryExpressionSource> Sources => sources.AsEnumerable();

		public bool IsEmpty => sources.Count == 0;

		public bool IsNaturalJoin => sources.Count > 1 && joinParts.Count == 0;

		private string NewSourceKey() {
			return (++sourceKey).ToString();
		}

		public void Source(SqlQueryExpressionSource source) {
			if (source == null)
				throw new ArgumentNullException(nameof(source));

			if (source.IsAliased &&
				aliases.Contains(source.Alias)) {
				throw new ArgumentException($"Another source defined the alias {source.Alias} in this command expression");
			}

			if (source.IsAliased)
				aliases.Add(source.Alias);

			source.UniqueKey = NewSourceKey();
			sources.Add(source);
		}

		public void Table(ObjectName tableName) {
			Table(tableName, null);
		}

		public void Table(ObjectName tableName, string alias) {
			Source(new SqlQueryExpressionSource(tableName, alias));
		}

		public void Query(SqlQueryExpression query, string alias) {
			Source(new SqlQueryExpressionSource(query, alias));
		}

		public void Join(JoinType joinType) {
			Join(joinType, null);
		}

		public void Join(JoinType joinType, SqlExpression onExpression) {
			if (sources.Count < 1)
				throw new InvalidOperationException("Cannot join on set that has no sources.");

			JoinPart part;

			var source = sources[sources.Count - 1];
			if (source.IsQuery) {
				part = new JoinPart(joinType, source.Query, onExpression);
			} else {
				part = new JoinPart(joinType, source.TableName, onExpression);
			}

			joinParts.Add(part);
		}

		public JoinPart GetJoinPart(int offset) {
			return joinParts[offset];
		}

		SqlQueryExpressionFrom ISqlExpressionPreparable<SqlQueryExpressionFrom>.Prepare(ISqlExpressionPreparer preparer) {
			var obj = new SqlQueryExpressionFrom();

			foreach (var part in joinParts) {
				var onExp = part.OnExpression;
				if (onExp != null) {
					onExp = onExp.Prepare(preparer);
				}

				if (part.IsQuery) {
					obj.joinParts.Add(new JoinPart(part.JoinType, part.Query, onExp));
				} else {
					obj.joinParts.Add(new JoinPart(part.JoinType, part.TableName, onExp));
				}
			}

			obj.aliases = new List<string>(aliases);

			foreach (var source in sources) {
				var prepared = source.Prepare(preparer);
				obj.sources.Add(prepared);
			}

			return obj;
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (IsEmpty)
				return;

			builder.Append("FROM ");

			for (int i = 0; i < sources.Count; i++) {
				var source = sources[i];

				JoinPart joinPart = null;

				if (i > 0) {
					if (!IsNaturalJoin) {
						joinPart = joinParts[i - 1];
						if (joinPart != null &&
						    joinPart.OnExpression != null) {
							if (joinPart.JoinType == JoinType.Inner) {
								builder.Append(" INNER JOIN ");
							} else if (joinPart.JoinType == JoinType.Right) {
								builder.Append(" RIGHT OUTER JOIN ");
							} else if (joinPart.JoinType == JoinType.Left) {
								builder.Append(" LEFT OUTER JOIN ");
							} else if (joinPart.JoinType == JoinType.Full) {
								builder.Append(" FULL OUTER JOINT ");
							}
						}
					} else {
						builder.Append(", ");
					}
				}

				source.AppendTo(builder);

				if (joinPart != null &&
				    joinPart.OnExpression != null) {
					builder.Append(" ON ");
					joinPart.OnExpression.AppendTo(builder);
				}
			}
		}
	}
}