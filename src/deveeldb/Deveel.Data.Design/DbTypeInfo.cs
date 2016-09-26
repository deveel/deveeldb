using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class DbTypeInfo {
		internal DbTypeInfo(TypeModelConfiguration configuration) {
			Configuration = configuration;
		}

		private TypeModelConfiguration Configuration { get; set; }

		public string TableName {
			get { return Configuration.TableName; }
		}

		public Type Type {
			get { return Configuration.Type; }
		}

		public DbMemberInfo KeyMember {
			get {
				var key = GetConstraint(ConstraintType.PrimaryKey);
				if (key == null)
					return null;

				return key.Members.FirstOrDefault();
			}
		}

		public DbMemberInfo GetMember(string memberName) {
			var buildInfo = Configuration.GetMember(memberName);
			if (buildInfo == null)
				return null;

			return new DbMemberInfo(buildInfo);
		}

		public DbConstraintInfo GetConstraint(ConstraintType constraintType) {
			return GetConstraint(null, constraintType);
		}

		public DbConstraintInfo GetConstraint(string name, ConstraintType constraintType) {
			var config = Configuration.GetConstraint(name, constraintType);
			if (config == null)
				return null;

			return new DbConstraintInfo(config);
		}

		internal object CreateObject(Row row) {
			if (row == null)
				throw new ArgumentNullException("row");

			// TODO: Construct parametrized objects

			var obj = Activator.CreateInstance(Type, true);

			foreach (var memberInfo in Configuration.MemberNames.Select(GetMember)) {
				memberInfo.ApplyFromRow(obj, row);
			}

			return obj;
		}

		private DbConstraintInfo SelectUniqueConstraint(object obj) {
			foreach (var constraint in Configuration.GetConstraints().Select(x => new DbConstraintInfo(x))) {
				bool toPick = true;
				foreach (var member in constraint.Members) {
					var value = member.GetFieldValue(obj);
					if (Field.IsNullField(value)) {
						toPick = false;
						break;
					}
				}

				if (toPick)
					return constraint;
			}

			return null;
		}

		private SqlExpression Where(object obj, IList<string> members, IList<KeyValuePair<string, Field>> parameters) {
			var key = KeyMember;
			if (key != null) {
				var varName = String.Format("p{0}", parameters.Count);
				var value = key.GetFieldValue(obj);

				if (Field.IsNullField(value))
					throw new InvalidOperationException();

				members.Add(key.Member.Name);
				parameters.Add(new KeyValuePair<string, Field>(varName, value));
				return SqlExpression.Equal(SqlExpression.Reference(new ObjectName(key.ColumnName)),
					SqlExpression.VariableReference(varName));
			}

			var uniqueConstraint = SelectUniqueConstraint(obj);
			if (uniqueConstraint == null)
				throw new InvalidOperationException();

			var uniqueKeys = uniqueConstraint.Members.ToArray();

			SqlExpression where = null;
			for (int i = 0; i < uniqueKeys.Length; i++) {
				var uniqueKey = uniqueKeys[i];

				var varName = String.Format("p{0}", parameters.Count - i);
				var value = uniqueKey.GetFieldValue(obj);

				if (Field.IsNullField(value))
					throw new InvalidOperationException();

				members.Add(uniqueKey.Member.Name);

				parameters.Add(new KeyValuePair<string, Field>(varName, value));

				var colRef = SqlExpression.Reference(new ObjectName(uniqueKey.ColumnName));
				var eq = SqlExpression.Equal(colRef, SqlExpression.VariableReference(varName));

				if (where == null) {
					where = eq;
				} else {
					where = SqlExpression.And(where, eq);
				}
			}

			return where;
		}

		private IEnumerable<SqlColumnAssignment> GenerateAssignments(object obj, IList<KeyValuePair<string, Field>> parameters,
			IList<string> exclude) {
			var assignments = new List<SqlColumnAssignment>();

			var members = Configuration.MemberNames.Select(GetMember).ToArray();
			for (int i = 0; i < members.Length; i++) {
				var member = members[i];

				if (exclude.Contains(member.Member.Name))
					continue;

				var value = member.GetFieldValue(obj);
				var varName = String.Format("p{0}", parameters.Count - i);

				parameters.Add(new KeyValuePair<string, Field>(varName, value));
				assignments.Add(new SqlColumnAssignment(member.ColumnName, SqlExpression.VariableReference(varName)));
			}

			return assignments;
		}

		internal SqlStatement GenerateUpdate(object obj, IList<KeyValuePair<string, Field>> parameters) {
			var filterMembers = new List<string>();
			var where = Where(obj, filterMembers, parameters);

			var assignments = GenerateAssignments(obj, parameters, filterMembers);

			return new UpdateStatement(ObjectName.Parse(TableName), where, assignments);
		}

		internal SqlStatement GenerateDelete(object obj, IList<KeyValuePair<string, Field>> parameters) {
			var filterMembers = new List<string>();
			var where = Where(obj, filterMembers, parameters);

			return new DeleteStatement(ObjectName.Parse(TableName), where);
		}

		internal SqlStatement GenerateInsert(object obj, IList<KeyValuePair<string, Field>> parameters) {
			var columnNames = new List<string>();
			var values = GenerateInsertValues(obj, columnNames, parameters);
			return new InsertStatement(ObjectName.Parse(TableName), columnNames.ToArray(), new SqlExpression[][] {values});
		}

		private SqlExpression[] GenerateInsertValues(object obj, IList<string> columnNames,
			IList<KeyValuePair<string, Field>> parameters) {

			var members = Configuration.MemberNames.Select(GetMember).ToArray();
			var expressions = new List<SqlExpression>();

			for (int i = 0; i < members.Length; i++) {
				var member = members[i];

				var value = member.GetFieldValue(obj);

				if (member.IsKey && 
					Field.IsNullField(value) &&
					!member.IsGenerated) {
					throw new InvalidOperationException();
				}

				if (member.IsGenerated)
					continue;

				var paramName = String.Format("p{0}", i);

				columnNames.Add(member.ColumnName);
				expressions.Add(SqlExpression.VariableReference(paramName));
				parameters.Add(new KeyValuePair<string, Field>(paramName, value));
			}

			return expressions.ToArray();
		}
	}
}
