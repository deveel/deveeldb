using System;
using System.Collections.Generic;
using System.Data.Common.CommandTrees;
using System.Data.Metadata.Edm;

using Deveel.Data.Client;

namespace Deveel.Data.Entity {
	internal abstract class ExpressionVisitor : DbExpressionVisitor<Expression> {
		public ExpressionVisitor() {
			Parameters = new List<DeveelDbParameter>();
		}

		protected string tabs = String.Empty;
		private int parameterCount = 1;
		protected ExpressionScope scope = new ExpressionScope();
		protected int propertyLevel;
		protected Dictionary<EdmMember, Expression> values;

		public List<DeveelDbParameter> Parameters { get; private set; }

		private Expression VisitBinaryExpression(DbExpression left, DbExpression right, string op) {
			return new BinaryExpression {
			                            	Left = left.Accept(this),
			                            	Operator = op,
			                            	Right = right.Accept(this),
			                            	WrapLeft = Wrap(left),
			                            	WrapRight = Wrap(right)
			                            };
		}


		private static bool Wrap(DbExpression exp) {
			if (exp.ExpressionKind == DbExpressionKind.Property ||
				exp.ExpressionKind == DbExpressionKind.ParameterReference ||
				exp.ExpressionKind == DbExpressionKind.Constant)
				return false;

			return true;
		}

		protected BranchExpression VisitInputExpression(DbExpression e, string name, TypeUsage type) {
			Expression expression = e.Accept(this);
			if (!(expression is BranchExpression))
				throw new InvalidOperationException();

			BranchExpression branchExpression = (BranchExpression)expression;
			branchExpression.Name = name;

			if (branchExpression is TableExpression && type != null)
				(branchExpression as TableExpression).Type = type;

			SelectExpression select = (SelectExpression) branchExpression;
			if (name != null) {
				if (!select.Scoped)
					scope.Add(name, select.From);
				else
					scope.Add(name, branchExpression);
			}

			return branchExpression;
		}

		protected void VisitNewInstanceExpression(SelectExpression select, DbNewInstanceExpression expression) {
			if (!(expression.ResultType.EdmType is RowType))
				throw new ArgumentException();

			RowType row = expression.ResultType.EdmType as RowType;

			for (int i = 0; i < expression.Arguments.Count; i++) {
				VariableExpression col = null;

				Expression exp = expression.Arguments[i].Accept(this);
				if (exp is VariableExpression)
					col = exp as VariableExpression;
				else {
					col = new VariableExpression();
					col.Text = exp;
				}

				col.ColumnAlias = row.Properties[i].Name;
				select.Columns.Add(col);
			}
		}


		protected string CreateUniqueParameterName() {
			return String.Format("@gp{0}", parameterCount++);
		}

		private VariableExpression GetColumnFromPropertyTree(PropertyExpression fragment) {
			int lastIndex = fragment.Properties.Count - 1;
			Expression currentFragment = scope.GetExpression(fragment.Properties[0]);
			if (currentFragment != null) {
				for (int i = 1; i < fragment.Properties.Count; i++) {
					Expression f = (currentFragment as BranchExpression).GetProperty(fragment.Properties[i]);
					if (f == null) break;
					currentFragment = f;
				}
				if (currentFragment is VariableExpression)
					return currentFragment as VariableExpression;
			}
			return new VariableExpression {TableName = null, ColumnName = fragment.Properties[lastIndex]};
		}

		public override Expression Visit(DbExpression expression) {
			throw new InvalidOperationException("Should never be reached.");
		}

		public override Expression Visit(DbAndExpression expression) {
			return VisitBinaryExpression(expression.Left, expression.Right, "AND");
		}

		public override Expression Visit(DbApplyExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbArithmeticExpression expression) {
			if (expression.ExpressionKind == DbExpressionKind.UnaryMinus) {
				ExpressionBuilder eb = new ExpressionBuilder();
				eb.Append("-(");
				eb.Append(expression.Arguments[0].Accept(this));
				eb.Append(")");
				return eb;
			}

			string op = String.Empty;
			switch (expression.ExpressionKind) {
				case DbExpressionKind.Divide:
					op = "/"; break;
				case DbExpressionKind.Minus:
					op = "-"; break;
				case DbExpressionKind.Modulo:
					op = "%"; break;
				case DbExpressionKind.Multiply:
					op = "*"; break;
				case DbExpressionKind.Plus:
					op = "+"; break;
				default:
					throw new NotSupportedException();
			}
			return VisitBinaryExpression(expression.Arguments[0], expression.Arguments[1], op);

		}

		public override Expression Visit(DbCaseExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbCastExpression expression) {
			return expression.Argument.Accept(this);
		}

		public override Expression Visit(DbComparisonExpression expression) {
			return VisitBinaryExpression(expression.Left, expression.Right, Metadata.GetOperator(expression.ExpressionKind));
		}

		public override Expression Visit(DbConstantExpression expression) {
			PrimitiveTypeKind pt = ((PrimitiveType)expression.ResultType.EdmType).PrimitiveTypeKind;
			string literal = Metadata.GetNumericLiteral(pt, expression.Value);
			if (literal != null)
				return new LiteralExpression(literal);
			else if (pt == PrimitiveTypeKind.Boolean)
				return new LiteralExpression(String.Format("cast({0} as numeric(0,0))",
					(bool)expression.Value ? 1 : 0));
			else {
				// use a parameter for non-numeric types so we get proper
				// quoting
				DeveelDbParameter p = new DeveelDbParameter();
				p.ParameterName = CreateUniqueParameterName();
				p.DbType = Metadata.GetDbType(expression.ResultType);
				p.Value = expression.Value;
				Parameters.Add(p);
				return new LiteralExpression(p.ParameterName);
			}
		}

		public override Expression Visit(DbCrossJoinExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbDerefExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbDistinctExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbElementExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbExceptExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbFilterExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbFunctionExpression expression) {
			//TODO: process a function call...
			throw new NotImplementedException();
		}

		public override Expression Visit(DbEntityRefExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbRefKeyExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbGroupByExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbIntersectExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbIsEmptyExpression expression) {
			ExistsExpression exp = new ExistsExpression(expression.Argument.Accept(this));
			exp.Negate();
			return exp;
		}

		public override Expression Visit(DbIsNullExpression expression) {
			IsNullExpression exp = new IsNullExpression();
			exp.Argument = expression.Argument.Accept(this);
			return exp;
		}

		public override Expression Visit(DbIsOfExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbJoinExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbLikeExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbLimitExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbNewInstanceExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbNotExpression expression) {
			NegatableExpression exp = expression.Argument.Accept(this) as NegatableExpression;
			if (exp == null)
				throw new InvalidOperationException();
			exp.Negate();
			return exp;
		}

		public override Expression Visit(DbNullExpression expression) {
			return new LiteralExpression("NULL");
		}

		public override Expression Visit(DbOfTypeExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbOrExpression expression) {
			return VisitBinaryExpression(expression.Left, expression.Right, "OR");
		}

		public override Expression Visit(DbParameterReferenceExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbProjectExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbPropertyExpression expression) {
			propertyLevel++;
			PropertyExpression fragment = (PropertyExpression) expression.Instance.Accept(this);
			fragment.Properties.Add(expression.Property.Name);
			propertyLevel--;

			// if we are not at the top level property then just return
			if (propertyLevel > 0) return fragment;

			// we are at the top level property so now we can do our work
			VariableExpression column = GetColumnFromPropertyTree(fragment);

			for (int i = fragment.Properties.Count - 1; i >= 0; --i) {
				BranchExpression inputFragment = scope.GetExpression(fragment.Properties[i]);
				if (inputFragment != null) {
					column.TableAlias = inputFragment.Name;
					break;
				}
			}
			return column;
		}

		public override Expression Visit(DbQuantifierExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbRefExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbRelationshipNavigationExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbScanExpression expression) {
			EntitySetBase target = expression.Target;
			TableExpression fragment = new TableExpression();

			MetadataProperty property;
			bool propExists = target.MetadataProperties.TryGetValue("DefiningQuery", true, out property);
			if (propExists && property.Value != null)
				fragment.DefiningQuery = new LiteralExpression(property.Value as string);
			else {
				fragment.Schema = target.EntityContainer.Name;
				fragment.Table = target.Name;

				propExists = target.MetadataProperties.TryGetValue("Schema", true, out property);
				if (propExists && property.Value != null)
					fragment.Schema = property.Value as string;
				propExists = target.MetadataProperties.TryGetValue("Table", true, out property);
				if (propExists && property.Value != null)
					fragment.Table = property.Value as string;
			}
			return fragment;
		}

		public override Expression Visit(DbSortExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbSkipExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbTreatExpression expression) {
			throw new NotImplementedException();
		}

		public override Expression Visit(DbUnionAllExpression expression) {
			throw new NotSupportedException();
		}

		public override Expression Visit(DbVariableReferenceExpression expression) {
			PropertyExpression propertyExpression = new PropertyExpression();
			propertyExpression.Properties.Add(expression.VariableName);
			return propertyExpression;
		}

		public abstract string BuildSqlStatement(DbCommandTree commandTree);
	}
}