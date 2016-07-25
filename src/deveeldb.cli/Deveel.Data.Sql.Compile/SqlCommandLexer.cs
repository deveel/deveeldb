using System;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.Sql.Compile {
	public sealed class SqlCommandLexer {
		private SqlParseState currentState;
		private Stack<SqlParseState> stateStack;

		public SqlCommandLexer() {
			currentState = new SqlParseState();
			stateStack = new Stack<SqlParseState>();
			RemoveComments = true;
		}

		public IParseState CurrentState {
			get { return currentState; }
		}

		public bool RemoveComments { get; set; }

		public void PushState() {
			stateStack.Push(currentState);
			currentState = new SqlParseState();
		}

		public void PopState() {
			currentState = stateStack.Pop();
		}

		public void Append(string s) {
			currentState.Input.Append(s);
		}

		public void Discard() {
			currentState.Input.Length = 0;
			currentState.Comments.Length = 0;
			currentState.State = SqlParseStateType.NewStatement;
		}

		public void Contined() {
			currentState.State = SqlParseStateType.Start;
		}

		public void Consumed() {
			currentState.State = SqlParseStateType.NewStatement;
		}

		public bool HasNext() {
			if (currentState.State == SqlParseStateType.PotentialEndFound)
				throw new InvalidOperationException("Illegal state found: invoke Continued() or Consumed() before.");
			if (currentState.Input.Length == 0)
				return false;

			ReadPartialInput();
			return currentState.State == SqlParseStateType.PotentialEndFound;
		}

		public string Next() {
			if (currentState.State != SqlParseStateType.PotentialEndFound)
				throw new InvalidOperationException("Invalid lex state.");

			return currentState.Input.ToString();
		}

		private void ReadPartialInput() {
			int pos = 0;
			char current;
			SqlParseStateType oldstate = SqlParseStateType.Unknown;

			// local variables: faster access.
			var state = currentState.State;
			bool lastEoline = currentState.IsEol;

			StringBuilder input = currentState.Input;
			StringBuilder parsed = currentState.Input;

			if (state == SqlParseStateType.NewStatement) {
				parsed.Length = 0;

				// skip leading white spaces of next statement...
				while (pos < input.Length && 
					Char.IsWhiteSpace(input[pos])) {
					var newLine = Environment.NewLine.ToCharArray();
					currentState.IsEol = input[pos] == '\n';
					++pos;
				}
				input.Remove(0, pos);
				pos = 0;
			}

			if (input.Length == 0) {
				state = SqlParseStateType.PotentialEndFound;
			}

			while (state != SqlParseStateType.PotentialEndFound && 
				pos < input.Length) {
				bool vetoAppend = false;
				bool reIterate;
				current = input[pos];
				if (current == '\r') {
					current = '\n'; // canonicalize.
				}

				if (current == '\n') {
					currentState.IsEol = true;
				}

				do {
					reIterate = false;
					switch (state) {
						case SqlParseStateType.NewStatement:
						// case START: START == STATEMENT.
						case SqlParseStateType.Statement:
							if (current == '\n') {
								state = SqlParseStateType.PotentialEndFound;
								currentState.IsEol = true;
							} else if (RemoveComments && lastEoline && current == ';') {
								/*
								 * special handling of the 'first-two-semicolons-after
								 * a-newline-comment'.
								 */
								state = SqlParseStateType.FirstSemicolonOnLine;
							} else if (!lastEoline && current == ';') {
								currentState.IsEol = false;
								state = SqlParseStateType.PotentialEndFound;
							} else if (RemoveComments && current == '/') {
								state = SqlParseStateType.StartComment;
							} else if (RemoveComments && lastEoline && current == '#') {
								/*
								 * only if '#' this is the first character, make it a
								 * comment..
								 */
								state = SqlParseStateType.EndLineComment;
							} else if (current == '"') {
								state = SqlParseStateType.String;
							} else if (current == '\'') {
								state = SqlParseStateType.SqlString;
							} else if (current == '-') {
								state = SqlParseStateType.StartAnsiString;
							} else if (current == '\\') {
								state = SqlParseStateType.StatementQuote;
							}
							break;
						case SqlParseStateType.StatementQuote:
							state = SqlParseStateType.Statement;
							break;
						case SqlParseStateType.FirstSemicolonOnLine:
							if (current == ';') {
								state = SqlParseStateType.EndLineComment;
							} else {
								state = SqlParseStateType.PotentialEndFound;
								current = ';';
								/*
								 * we've read too much. Reset position.
								 */
								--pos;
							}
							break;
						case SqlParseStateType.StartComment:
							if (current == '*') {
								state = SqlParseStateType.Comment;
							} else {
								parsed.Append('/');
								state = SqlParseStateType.Statement;
								reIterate = true;
							}
							break;
						case SqlParseStateType.Comment:
							if (current == '*') {
								state = SqlParseStateType.PreEndComment;
							}
							break;
						case SqlParseStateType.PreEndComment:
							if (current == '/') {
								state = SqlParseStateType.Statement;
							} else if (current == '*') {
								state = SqlParseStateType.PreEndComment;
							} else {
								state = SqlParseStateType.Comment;
							}
							break;
						case SqlParseStateType.StartAnsiString:
							if (current == '-') {
								state = SqlParseStateType.EndLineComment;
							} else {
								parsed.Append('-');
								state = SqlParseStateType.Statement;
								reIterate = true;
							}
							break;
						case SqlParseStateType.EndLineComment:
							if (current == '\n') {
								state = SqlParseStateType.PotentialEndFound;
							}
							break;
						case SqlParseStateType.String:
							if (current == '\\') {
								state = SqlParseStateType.SqlStringQuote;
							} else if (current == '"') {
								state = SqlParseStateType.Statement;
							}
							break;
						case SqlParseStateType.SqlString:
							if (current == '\\') {
								state = SqlParseStateType.SqlStringQuote;
							}
							if (current == '\'') {
								state = SqlParseStateType.Statement;
							}
							break;
						case SqlParseStateType.StringQuote:
							vetoAppend = current == '\n'; // line continuation
							if (current == 'n') {
								current = '\n';
							} else if (current == 'r') {
								current = '\r';
							} else if (current == 't') {
								current = '\t';
							} else if (current == '\\') {
								// noop
							} else if (current != '\n' && current != '"') {
								// if we do not recognize the escape sequence,
								// pass it through.
								parsed.Append("\\");
							}
							state = SqlParseStateType.String;
							break;
						case SqlParseStateType.SqlStringQuote:
							vetoAppend = current == '\n'; // line continuation
														  // convert a "\'" to a correct SQL-Quote "''"
							if (current == '\'') {
								parsed.Append("'");
							} else if (current == 'n') {
								current = '\n';
							} else if (current == 'r') {
								current = '\r';
							} else if (current == 't') {
								current = '\t';
							} else if (current == '\\') {
								// noop
							} else if (current != '\n') {
								// if we do not recognize the escape sequence,
								// pass it through.
								parsed.Append("\\");
							}
							state = SqlParseStateType.SqlString;
							break;
					}
				} while (reIterate);

				/* append to parsed; ignore comments */
				if (!vetoAppend && 
					(state == SqlParseStateType.Statement && 
					oldstate != SqlParseStateType.PreEndComment || 
					state == SqlParseStateType.NewStatement || 
					state == SqlParseStateType.StatementQuote || 
					state == SqlParseStateType.String || 
					state == SqlParseStateType.SqlString || 
					state == SqlParseStateType.PotentialEndFound)) {
					parsed.Append(current);
				}

				oldstate = state;
				pos++;
				/*
				 * we maintain the state of 'just seen newline' as long as we only
				 * skip whitespaces..
				 */
				lastEoline &= Char.IsWhiteSpace(current);
			}
			// we reached: POTENTIAL_END_FOUND. Store the rest, that
			// has not been parsed in the input-buffer.
			input.Remove(0, pos);
			currentState.State = state;
		}
	}
}
