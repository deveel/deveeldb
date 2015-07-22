// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// The default implementation of a <see cref="ISqlNodeVisitor"/>
	/// that implements the visitor as a protected accessor.
	/// </summary>
	class SqlNodeVisitor : ISqlNodeVisitor {
		/// <summary>
		/// Visits the given SQL node.
		/// </summary>
		/// <param name="node">The <see cref="ISqlNode"/> to visit.</param>
		/// <seealso cref="ISqlNodeVisitor.Visit"/>
		public virtual void Visit(ISqlNode node) {
			if (node is IntegerLiteralNode) {
				VisitIntegerLiteral((IntegerLiteralNode) node);
			} else if (node is NumberLiteralNode) {
				VisitNumberLiteral((NumberLiteralNode) node);
			} else if (node is StringLiteralNode) {
				VisitStringLiteral((StringLiteralNode) node);
			} else if (node is DataTypeNode) {
				VisitDataType((DataTypeNode) node);
			} else if (node is IExpressionNode) {
				VisitExpression((IExpressionNode) node);
			} else if (node is IStatementNode) {
				VisitStatement((IStatementNode) node);
			} else if (node is ISqlVisitableNode) {
				((ISqlVisitableNode) node).Accept(this);
			}
		}

		public virtual void VisitStringLiteral(StringLiteralNode node) {
		}

		public virtual void VisitNumberLiteral(NumberLiteralNode node) {
		}

		public virtual void VisitIntegerLiteral(IntegerLiteralNode node) {
		}

		public virtual void VisitDataType(DataTypeNode node) {
		}

		public virtual void VisitExpression(IExpressionNode node) {
			if (node == null)
				return;
			
			if (node is SqlConstantExpressionNode) {
				VisitConstantExpression((SqlConstantExpressionNode) node);
			} else if (node is SqlReferenceExpressionNode) {
				VisitReferenceExpression((SqlReferenceExpressionNode) node);
			} else if (node is SqlVariableRefExpressionNode) {
				VisitVariableRefExpression((SqlVariableRefExpressionNode) node);
			} else if (node is SqlBetweenExpressionNode) {
				VisitBetweenExpression((SqlBetweenExpressionNode) node);
			} else if (node is SqlCaseExpressionNode) {
				VisitCaseExpression((SqlCaseExpressionNode) node);
			} else if (node is SqlFunctionCallExpressionNode) {
				VisitFunctionCall((SqlFunctionCallExpressionNode) node);
			} else if (node is SqlExpressionTupleNode) {
				VisitTupleExpression((SqlExpressionTupleNode) node);
			} else if (node is SqlBinaryExpressionNode) {
				VisitBinaryExpression((SqlBinaryExpressionNode) node);
			} else if (node is SqlUnaryExpressionNode) {
				VisitUnaryExpression((SqlUnaryExpressionNode) node);
			} else if (node is SqlQueryExpressionNode) {
				VisitQueryExpression((SqlQueryExpressionNode) node);
			} else {
				throw new InvalidOperationException(String.Format("The expression node of type '{0}' is invalid.", node.GetType()));
			}
		}

		public virtual void VisitNodeList(IEnumerable<ISqlNode> nodes) {
			foreach (var node in nodes) {
				Visit(node);
			}
		}

		#region Expressions

		public virtual void VisitQueryExpression(SqlQueryExpressionNode node) {
		}

		public virtual void VisitTupleExpression(SqlExpressionTupleNode node) {
			var exps = node.Expressions;
			if (exps != null)
				VisitNodeList(exps.Cast<ISqlNode>());
		}

		public virtual void VisitUnaryExpression(SqlUnaryExpressionNode node) {
		}

		public virtual void VisitBinaryExpression(SqlBinaryExpressionNode node) {
		}

		public virtual void VisitFunctionCall(SqlFunctionCallExpressionNode node) {
		}

		public virtual void VisitCaseExpression(SqlCaseExpressionNode node) {
		}

		public virtual void VisitBetweenExpression(SqlBetweenExpressionNode node) {
		}

		public virtual void VisitVariableRefExpression(SqlVariableRefExpressionNode node) {
		}

		public virtual void VisitConstantExpression(SqlConstantExpressionNode node) {
		}

		public virtual void VisitReferenceExpression(SqlReferenceExpressionNode node) {
		}

		#endregion

		#region Statements

		public virtual void VisitStatement(IStatementNode node) {
			if (node is CreateTableNode)
				VisitCreateTable((CreateTableNode) node);
			if (node is CreateViewNode)
				VisitCreateView((CreateViewNode) node);
			if (node is CreateTriggerNode) {
				VisitCreateTrigger((CreateTriggerNode) node);
			} else if (node is SelectStatementNode) {
				VisitSelect((SelectStatementNode) node);
			} else if (node is UpdateStatementNode) {
				VisitUpdate((UpdateStatementNode) node);
			} else if (node is InsertStatementNode) {
				VisitInsert((InsertStatementNode) node);
			}
		}

		public virtual void VisitSelect(SelectStatementNode node) {
			var exp = node.QueryExpression;
			if (exp != null)
				VisitExpression(exp);
		}

		public virtual  void VisitCreateTrigger(CreateTriggerNode node) {
			if (node.ProcedureArguments != null)
				VisitNodeList(node.ProcedureArguments);

			// TODO: handle the body
		}

		public virtual void VisitCreateView(CreateViewNode node) {
		}

		public virtual void VisitCreateTable(CreateTableNode node) {
			if (node.Columns != null)
				VisitTableColumns(node.Columns);
			if (node.Constraints != null)
				VisitTableConstraints(node.Constraints);
		}

		public virtual void VisitAlterTable(AlterTableNode node) {
			if (node.CreateTable != null)
				VisitCreateTable(node.CreateTable);

			if (node.Actions != null) {
				foreach (var action in node.Actions) {
					VisitAlterTableAction(action);
				}
			}
		}

		public virtual void VisitAlterTableAction(AlterTableActionNode action) {
		}

		public virtual void VisitTableConstraints(IEnumerable<TableConstraintNode> constraints) {
			foreach (var constraint in constraints) {
				VisitTableConstraint(constraint);
			}
		}

		public virtual void VisitTableConstraint(TableConstraintNode arg) {
			
		}

		public virtual void VisitTableColumns(IEnumerable<TableColumnNode> columnNodes) {
		}

		public virtual void VisitUpdate(UpdateStatementNode node) {
			if (node.SimpleUpdate != null)
				VisitSimpleUpdate(node.SimpleUpdate);
			if (node.QueryUpdate != null)
				VisitQueryUpdate(node.QueryUpdate);
		}

		public virtual void VisitSimpleUpdate(SimpleUpdateNode node) {
			
		}

		public virtual void VisitQueryUpdate(QueryUpdateNode node) {
			
		}


		public virtual void VisitInsert(InsertStatementNode node) {
			if (node.ValuesInsert != null)
				VisitValuesInsert(node.ValuesInsert);
		}

		protected virtual void VisitValuesInsert(ValuesInsertNode valuesInsert) {
			
		}

		#endregion
	}
}