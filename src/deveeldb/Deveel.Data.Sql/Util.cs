// 
//  Copyright 2010  Deveel
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

using System;
using System.Text;

using Deveel.Data.Functions;
using Deveel.Math;

namespace Deveel.Data.Sql {
	///<summary>
	/// Various utility methods for the iterpreter.
	///</summary>
	class Util {

		private static readonly TObject ZeroNumber = TObject.GetInt4(0);

		///<summary>
		/// Returns the Token as a non quoted reference.
		///</summary>
		///<param name="token"></param>
		/// <remarks>
		/// For example, a <see cref="SQLConstants.QUOTED_VARIABLE"/> token 
		/// will have the first and last <c>"</c> character removed. A 
		/// <see cref="SQLConstants.QUOTED_DELIMINATED_REF"/> will have <c>"</c> 
		/// removed in each deliminated section. For example, <c>"re1"."re2"."a"</c> 
		/// becomes <c>re1.re2.a</c> and <c>"re1.re2.a"</c> becomes <c>re1.re2.a</c>.
		/// </remarks>
		///<returns></returns>
		public static String AsNonQuotedRef(Token token) {
			if (token.kind == SQLConstants.QUOTED_VARIABLE) {
				// Strip " from start and end if a quoted variable
				return token.image.Substring(1, token.image.Length - 2);
			} else if (token.kind == SQLConstants.QUOTED_DELIMINATED_REF ||
					 token.kind == SQLConstants.QUOTEDGLOBVARIABLE) {
				// Remove all " from the string
				String image = token.image;
				StringBuilder b = new StringBuilder();
				int sz = image.Length;
				for (int i = 0; i < sz; ++i) {
					char c = image[i];
					if (c != '\"') {
						b.Append(c);
					}
				}
				return b.ToString();
			} else {
				return token.image;
			}
		}

		///<summary>
		/// Converts a Token which is either a <see cref="SQLConstants.STRING_LITERAL"/>, 
		/// <see cref="SQLConstants.NUMBER_LITERAL"/> or <see cref="SQLConstants.IDENTIFIER"/> 
		/// into an <see cref="object"/>.
		///</summary>
		///<param name="token"></param>
		///<param name="upper_identifiers">If is true then all identifiers are made upper case 
		/// before being returned (eg. if the object returns is a <see cref="VariableName"/> object).</param>
		///<returns></returns>
		public static Object ToParamObject(Token token, bool upper_identifiers) {
			if (token.kind == SQLConstants.STRING_LITERAL) {
				String raw_string = token.image.Substring(1, token.image.Length - 2);
				return TObject.GetString(EscapeTranslated(raw_string));
			}
				//    else if (token.kind == SQLConstants.NUMBER_LITERAL) {
				//      return TObject.GetBigNumber(BigNumber.Parse(token.image));
				//    }
			else if (token.kind == SQLConstants.BOOLEAN_LITERAL) {
				return TObject.GetBoolean(String.Compare(token.image, "true", true) == 0);
			} else if (token.kind == SQLConstants.NULL_LITERAL) {
				return TObject.Null;
			} else if (token.kind == SQLConstants.REGEX_LITERAL) {
				// Horrible hack,
				// Get rid of the 'regex' string at the start,
				String str = token.image.Substring(5).Trim();
				return TObject.GetString(str);
			} else if (token.kind == SQLConstants.QUOTED_VARIABLE ||
					   token.kind == SQLConstants.GLOBVARIABLE ||  // eg. Part.*
					   token.kind == SQLConstants.IDENTIFIER ||
					   token.kind == SQLConstants.DOT_DELIMINATED_REF ||
					   token.kind == SQLConstants.QUOTED_DELIMINATED_REF) {
				String name = AsNonQuotedRef(token);
				//      if (token.kind == SQLConstants.QUOTED_VARIABLE) {
				//        name = token.image.substring(1, token.image.length() - 1);
				//      }
				//      else {
				//        name = token.image;
				//      }
				if (upper_identifiers) {
					name = name.ToUpper();
				}
				VariableName v;
				int div = name.LastIndexOf(".");
				if (div != -1) {
					// Column represents '[something].[name]'
					// Check if the column name is an alias.
					String column_name = name.Substring(div + 1);
					// Make the '[something]' into a TableName
					TableName table_name = TableName.Resolve(name.Substring(0, div));

					// Set the variable name
					v = new VariableName(table_name, column_name);
				} else {
					// Column represents '[something]'
					v = new VariableName(name);
				}
				return v;
			} else {  // Otherwise it must be a reserved word, so just return the image
				// as a variable.
				String name = token.image;
				if (upper_identifiers) {
					name = name.ToUpper();
				}
				return new VariableName(token.image);
			}
		}

		/// <summary>
		/// Returns numeric 0
		/// </summary>
		public static TObject Zero {
			get { return ZeroNumber; }
		}

		///<summary>
		/// Parses a <see cref="SQLConstants.NUMBER_LITERAL"/> Token with a sign 
		/// boolean.
		///</summary>
		///<param name="token"></param>
		///<param name="negative"></param>
		///<returns></returns>
		public static TObject ParseNumberToken(Token token, bool negative) {
			return negative
			       	? TObject.GetBigNumber(BigNumber.Parse("-" + token.image))
			       	: TObject.GetBigNumber(BigNumber.Parse(token.image));
		}

		///<summary>
		/// Converts an expression array to an array type that can be added to an expression.
		///</summary>
		///<param name="arr"></param>
		///<returns></returns>
		public static TObject ToArrayParamObject(Expression[] arr) {
			return new TObject(TType.ArrayType, arr);
		}

		///<summary>
		/// Returns an array of <see cref="Expression"/> objects as a comma deliminated string.
		///</summary>
		///<param name="list"></param>
		///<returns></returns>
		public static String ExpressionListToString(Expression[] list) {
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < list.Length; ++i) {
				buf.Append(list[i].Text.ToString());
				if (i < list.Length - 1) {
					buf.Append(", ");
				}
			}
			return buf.ToString();
		}

		///<summary>
		/// Normalizes the <see cref="Expression"/> by removing all <c>NOT</c> 
		/// operators and altering the expression as appropriate. 
		///</summary>
		///<param name="exp"></param>
		/// <example>
		/// For example, the expression:
		/// <code>
		///    not ((a + b) = c and c = 5)
		/// </code>
		/// would be normalized to:
		/// <code>
		///   (a + b) &lt;&gt; c or c &lt;&gt; 5
		/// </code>
		/// </example>
		///<returns></returns>
		public static Expression Normalize(Expression exp) {
			// Only normalize if the expression contains a NOT operator.
			return exp.ContainsNotOperator() ? Normalize(exp, false) : exp;
		}

		///<summary>
		/// Normalizes the <see cref="Expression"/> by removing all <c>NOT</c> 
		/// operators and altering the expression as appropriate. 
		///</summary>
		///<param name="exp"></param>
		/// <example>
		/// For example, the expression:
		/// <code>
		///    not ((a + b) = c and c = 5)
		/// </code>
		/// would be normalized to:
		/// <code>
		///   (a + b) &lt;&gt; c or c &lt;&gt; 5
		/// </code>
		/// </example>
		///<returns></returns>
		private static Expression Normalize(Expression exp, bool inverse) {
			if (exp.Count <= 1) {
				if (inverse) {
					return StandardInverse(exp);
				} else {
					return exp;
				}
			}
			Operator op = (Operator)exp.Last;
			Expression[] exps = exp.Split();

			if (op.IsNot) {
				// If the operator is NOT then return the normalized form of the LHS.
				// We toggle the inverse flag.
				return Normalize(exps[0], !inverse);
			} else if (op.IsNotInversible) {
				// If the operator is not inversible, return the expression with a
				// '= false' if nothing else is possible
				Expression resolved_expr =
					   new Expression(Normalize(exps[0], false), op,
									  Normalize(exps[1], false));
				if (inverse) {
					return StandardInverse(resolved_expr);
				} else {
					return resolved_expr;
				}
			} else if (op.IsLogical) {
				// If logical we inverse the operator and inverse the left and right
				// side of the operator also.
				if (inverse) {
					return new Expression(Normalize(exps[0], inverse), op.Inverse(),
										  Normalize(exps[1], inverse));
				} else {
					return new Expression(Normalize(exps[0], inverse), op,
										  Normalize(exps[1], inverse));

				}
			} else {
				// By this point we can assume the operator is naturally inversible.
				if (inverse) {
					return new Expression(Normalize(exps[0], false), op.Inverse(),
										  Normalize(exps[1], false));
				} else {
					return new Expression(Normalize(exps[0], false), op,
										  Normalize(exps[1], false));
				}
			}

		}

		/// <summary>
		/// Returns an expression that is (exp) = false which is the natural 
		/// inverse of all expressions. 
		/// </summary>
		/// <param name="exp"></param>
		/// <remarks>
		/// This should only be used if the expression can't be inversed in any other way.
		/// </remarks>
		/// <returns></returns>
		private static Expression StandardInverse(Expression exp) {
			return new Expression(exp, Operator.Get("="), new Expression(TObject.GetBoolean(false)));
		}

		///<summary>
		/// Returns a <see cref="FunctionDef"/> object that represents the name and expression 
		/// list (of parameters) of a function.
		///</summary>
		///<param name="name"></param>
		///<param name="exp_list"></param>
		///<returns></returns>
		/// <exception cref="Exception">
		/// Thrown if the function doesn't exist.
		/// </exception>
		public static FunctionDef ResolveFunctionName(String name, Expression[] exp_list) {
			return new FunctionDef(name, exp_list);
		}

		/**
		 * Translate a string with escape codes into a un-escaped string.  \' is
		 * converted to ', \n is a newline, \t is a tab, \\ is \, etc.
		 */
		private static string EscapeTranslated(String input) {
			StringBuilder result = new StringBuilder();
			int size = input.Length;
			bool last_char_escape = false;
			bool last_char_quote = false;
			for (int i = 0; i < size; ++i) {
				char c = input[i];
				if (last_char_quote) {
					last_char_quote = false;
					if (c != '\'') {
						result.Append(c);
					}
				} else if (last_char_escape) {
					if (c == '\\') {
						result.Append('\\');
					} else if (c == '\'') {
						result.Append('\'');
					} else if (c == 't') {
						result.Append('\t');
					} else if (c == 'n') {
						result.Append('\n');
					} else if (c == 'r') {
						result.Append('\r');
					} else {
						result.Append('\\');
						result.Append(c);
					}
					last_char_escape = false;
				} else if (c == '\\') {
					last_char_escape = true;
				} else if (c == '\'') {
					last_char_quote = true;
					result.Append(c);
				} else {
					result.Append(c);
				}
			}
			return result.ToString();
		}

	}
}