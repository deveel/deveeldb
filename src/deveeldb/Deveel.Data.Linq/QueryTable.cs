using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Linq {
	public sealed class QueryTable<TType> : IQueryTable<TType>, IUpdateQueryTable where TType : class {
		private IQueryable<TType> queryable;

		internal QueryTable(SessionQueryContext context, IQueryable<TType> queryable) {
			this.queryable = queryable;
			Context = context;
		}

		~QueryTable() {
			Dispose(false);
		}

		private SessionQueryContext Context { get; set; }

		IEnumerator<TType> IEnumerable<TType>.GetEnumerator() {
			return queryable.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return queryable.GetEnumerator();
		}

		Expression IQueryable.Expression {
			get { return queryable.Expression; }
		}

		Type IQueryable.ElementType {
			get { return queryable.ElementType; }
		}

		IQueryProvider IQueryable.Provider {
			get { return queryable.Provider; }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (queryable != null &&
					(queryable is IDisposable))
					(queryable as IDisposable).Dispose();
			}

			Context = null;
			queryable = null;
		}

		Type IQueryTable.Type {
			get { return queryable.ElementType; }
		}

		public TType FindByKey(object key) {
			var typeInfo = Context.FindTypeInfo(typeof(TType));
			var keyMember = typeInfo.KeyMember;
			if (keyMember == null)
				throw new NotSupportedException(String.Format("The type '{0}' has not key configured.", typeInfo.Type));

			var parameter = Expression.Parameter(typeof(TType), "x");
			var body = Expression.Equal(Expression.MakeMemberAccess(parameter, keyMember.Member),
				Expression.Constant(key));
			var expression = Expression.Lambda<Func<TType, bool>>(body, parameter);

			var whereMethod = typeof(Queryable).GetMethods()
				.First(x => x.Name == "Where" &&
				            x.GetParameters().Length == 2 &&
							x.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(Expression<>));

			whereMethod = whereMethod.MakeGenericMethod(typeof(TType));

			var filter = Expression.Call(null, whereMethod, Expression.Constant(this), expression);

			return queryable.Provider.CreateQuery<TType>(filter).FirstOrDefault();
		}

		object IQueryTable.FindByKey(object key) {
			return FindByKey(key);
		}

		object IUpdateQueryTable.Insert(object obj) {
			return Insert((TType) obj);
		}

		public TType Insert(TType obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			throw new NotImplementedException();
		}

		bool IUpdateQueryTable.Update(object obj) {
			return Update((TType) obj);
		}

		public bool Update(TType obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			var typeInfo = Context.FindTypeInfo(typeof(TType));
			var parameters = new List<KeyValuePair<string, Field>>();
			var statement = typeInfo.GenerateUpdate(obj, parameters);

			using (var query = Context.CreateQuery()) {
				foreach (var parameter in parameters) {
					query.DeclareConstantVariable(parameter.Value, parameter.Value.Type, SqlExpression.Constant(parameter.Value));
				}

				return Result(query.ExecuteStatement(statement));
			}
		}

		private bool Result(StatementResult result) {
			if (result.Type == StatementResultType.Exception)
				throw result.Error;

			if (result.Type != StatementResultType.Result)
				throw new InvalidOperationException();

			var rowCount = result.Result.GetValue(0, 0);
			if (Field.IsNullField(rowCount))
				throw new InvalidOperationException();

			return rowCount.CastTo(PrimitiveTypes.Integer()) > 0;
		}

		bool IUpdateQueryTable.Delete(object obj) {
			return Delete((TType) obj);
		}

		public bool Delete(TType obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			var typeInfo = Context.FindTypeInfo(typeof(TType));
			var parameters = new List<KeyValuePair<string, Field>>();
			var statement = typeInfo.GenerateDelete(obj, parameters);

			using (var query = Context.CreateQuery()) {
				statement = statement.Prepare(new ParameterExpressionPreparer(parameters));

				return Result(query.ExecuteStatement(statement));
			}
		}

		#region ParameterExpressionPreparer

		class ParameterExpressionPreparer : IExpressionPreparer {
			private readonly IDictionary<string, Field> parameters;

			public ParameterExpressionPreparer(IEnumerable<KeyValuePair<string, Field>> parameters) {
				this.parameters = parameters.ToDictionary(x => x.Key, y => y.Value);
			}

			public bool CanPrepare(SqlExpression expression) {
				return expression is SqlVariableReferenceExpression;
			}

			public SqlExpression Prepare(SqlExpression expression) {
				var variableExp = (SqlVariableReferenceExpression) expression;

				Field value;
				if (!parameters.TryGetValue(variableExp.VariableName, out value))
					throw new InvalidOperationException();

				return SqlExpression.Constant(value);
			}
		}

		#endregion
	}
}
