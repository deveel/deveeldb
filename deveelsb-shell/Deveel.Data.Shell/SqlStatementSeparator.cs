using System;
using System.Collections;
using System.Text;

using Deveel.Shell;

namespace Deveel.Data.Shell {
	class SqlStatementSeparator : IEnumerator {
		#region ctor
		public SqlStatementSeparator() {
			currentState = new ParseState();
			stateStack = new Stack();
			removeComments = true;
		}
		#endregion

		#region Fields
		private bool removeComments;
		private ParseState currentState;
		private Stack stateStack;
		#endregion

		#region Properties
		public string Current {
			get {
				if (currentState.Type != TokenType.PotentialEndFound)
					throw new InvalidOperationException("Current called without MoveNext()");
				return currentState.CommandBuffer.ToString();
			}
		}
		#endregion

		#region IEnumerator Implementations
		object IEnumerator.Current {
			get { return Current; }
		}

		void IEnumerator.Reset() {
		}
		#endregion

		#region Private Methods
		private void ParsePartialInput() {
			int pos = 0;
			char current;
			TokenType oldstate = new TokenType();

			// local variables: faster access.
			TokenType state = currentState.Type;
			bool lastEoline = currentState.NewlineSeen;

			StringBuilder input = currentState.InputBuffer;
			StringBuilder parsed = currentState.CommandBuffer;

			if (state == TokenType.NewStatement) {
				parsed.Length = 0;
				// skip leading whitespaces of next statement...
				while (pos < input.Length &&
					Char.IsWhiteSpace(input[pos])) {
					//CHECK: what about \r?
					currentState.NewlineSeen = (input[pos] == '\n');
					++pos;
				}
				input.Remove(0, pos);
				pos = 0;
			}

			if (input.Length == 0)
				state = TokenType.PotentialEndFound;

			// Console.Error.WriteLine("Startstate: " + state + "; LEOL: " + lastEoline);

			while (state != TokenType.PotentialEndFound && pos < input.Length) {
				bool vetoAppend = false;
				bool reIterate;
				current = input[pos];
				if (current == '\r')
					current = '\n'; // canonicalize.

				if (current == '\n')
					currentState.NewlineSeen = true;

				// Console.Out.Write("Pos: " + pos + "\t");
				do {
					reIterate = false;
					switch (state) {
						case TokenType.NewStatement:
						//case START: START == STATEMENT.
						case TokenType.Statement:
							if (current == '\n') {
								state = TokenType.PotentialEndFound;
								currentState.NewlineSeen = true;
							}

							// special handling of the 'first-two-semicolons-after
								// a-newline-comment'.
							else if (removeComments && lastEoline && current == ';') {
								state = TokenType.FirstSemicolonOnLineSeen;
							} else if (!lastEoline && current == ';') {
								currentState.NewlineSeen = false;
								state = TokenType.PotentialEndFound;
							} else if (removeComments && current == '/') {
								state = TokenType.StartComment;
							}

							// only if '#' this is the first character, make it
							// a comment..
							else if (removeComments && lastEoline && current == '#') {
								state = TokenType.EndLineComment;
							} else if (current == '"') {
								state = TokenType.String;
							} else if (current == '\'') {
								state = TokenType.SqlString;
							} else if (current == '-') {
								state = TokenType.StartAnsi;
							} else if (current == '\\') {
								state = TokenType.StatementQuote;
							}
							break;
						case TokenType.StatementQuote:
							state = TokenType.Statement;
							break;
						case TokenType.FirstSemicolonOnLineSeen:
							if (current == ';') {
								state = TokenType.EndLineComment;
							} else {
								state = TokenType.PotentialEndFound;
								current = ';';
								// we've read too much. Reset position.
								--pos;
							}
							break;
						case TokenType.StartComment:
							if (current == '*') {
								state = TokenType.Comment;
								// } else if (current == '/') {
								// state = TokenType.EndLineComment;
							} else {
								parsed.Append('/');
								state = TokenType.Statement;
								reIterate = true;
							}
							break;
						case TokenType.Comment:
							if (current == '*')
								state = TokenType.PreEndComment;
							break;
						case TokenType.PreEndComment:
							if (current == '/') {
								state = TokenType.Statement;
							} else if (current == '*') {
								state = TokenType.PreEndComment;
							} else {
								state = TokenType.Comment;
							}
							break;
						case TokenType.StartAnsi:
							if (current == '-') {
								state = TokenType.EndLineComment;
							} else {
								parsed.Append('-');
								state = TokenType.Statement;
								reIterate = true;
							}
							break;
						case TokenType.EndLineComment:
							if (current == '\n')
								state = TokenType.PotentialEndFound;
							break;
						case TokenType.String:
							if (current == '\\') {
								state = TokenType.StringQuote;
							} else if (current == '"') {
								state = TokenType.Statement;
							}
							break;
						case TokenType.SqlString:
							if (current == '\\')
								state = TokenType.SqlStringQuote;
							if (current == '\'')
								state = TokenType.Statement;
							break;
						case TokenType.StringQuote:
							if (current == '\"')
								parsed.Append("\"");
							vetoAppend = (current == '\n');
							if (current == 'n') {
								current = '\n';
							} else if (current == 'r') {
								current = '\r';
							} else if (current == 't') {
								current = '\t';
							} else if (current != '\n' && current != '"') {
								// if we do not recognize the escape sequence,
								// pass it through.
								parsed.Append("\\");
							}

							state = TokenType.String;
							break;
						case TokenType.SqlStringQuote:
							if (current == '\'')
								parsed.Append("'");
							vetoAppend = (current == '\n');
							// convert a "\'" to a correct SQL-Quote "''"
							if (current == '\'') {
								parsed.Append("'");
							} else if (current == 'n') {
								current = '\n';
							} else if (current == 'r') {
								current = '\r';
							} else if (current == 't') {
								current = '\t';
							} else if (current != '\n') {
								// if we do not recognize the escape sequence,
								// pass it through.
								parsed.Append("\\");
							}
							state = TokenType.SqlString;
							break;
					}
				} while (reIterate);

				// append to parsed; ignore comments
				if (!vetoAppend &&
					((state == TokenType.Statement && oldstate != TokenType.PreEndComment) ||
					state == TokenType.NewStatement ||
					state == TokenType.StatementQuote ||
					state == TokenType.String ||
					state == TokenType.SqlString ||
					state == TokenType.PotentialEndFound)) {
					parsed.Append(current);
				}

				oldstate = state;
				pos++;
				// we maintain the state of 'just seen newline' as long
				// as we only skip whitespaces..
				lastEoline &= Char.IsWhiteSpace(current);
			}
			// we reached: POTENTIAL_END_FOUND. Store the rest, that
			// has not been parsed in the input-buffer.
			input.Remove(0, pos);
			currentState.Type = state;
		}
		#endregion

		#region Public Methods
		public PropertyHolder GetRemoveCommentsProperty() {
			return new RemoveCommentsProperty(this);
		}

		public void Push() {
			stateStack.Push(currentState);
			currentState = new ParseState();
		}

		public void Pop() {
			currentState = (ParseState)stateStack.Pop();
		}

		public void Append(string s) {
			currentState.InputBuffer.Append(s);
		}

		public void Discard() {
			currentState.InputBuffer.Length = 0;
			currentState.CommandBuffer.Length = 0;
			currentState.Type = TokenType.NewStatement;
		}

		public void Cont() {
			currentState.Type = TokenType.Start;
		}

		public void Consumed() {
			currentState.Type = TokenType.NewStatement;
		}

		public bool MoveNext() {
			if (currentState.Type == TokenType.PotentialEndFound)
				throw new InvalidOperationException("call Cont() or Consumed() before MoveNext()");
			if (currentState.InputBuffer.Length == 0)
				return false;
			ParsePartialInput();
			return (currentState.Type == TokenType.PotentialEndFound);
		}

		public void RemoveComments(bool b) {
			removeComments = b;
		}
		#endregion

		#region ParseState
		class ParseState {
			#region ctor
			public ParseState() {
				_eolineSeen = true; // we start with a new line.
				_state = TokenType.NewStatement;
				_inputBuffer = new StringBuilder();
				_commandBuffer = new StringBuilder();
			}
			#endregion

			#region Fields
			private TokenType _state;
			private StringBuilder _inputBuffer;
			private StringBuilder _commandBuffer;
			/*
			 * instead of adding new states, we store the
			 * fact, that the last 'potential_end_found' was
			 * a newline here.
			 */
			private bool _eolineSeen;
			#endregion

			#region Properties
			public TokenType Type {
				get { return _state; }
				set { _state = value; }
			}

			public bool NewlineSeen {
				get { return _eolineSeen; }
				set { _eolineSeen = value; }
			}

			public StringBuilder InputBuffer {
				get { return _inputBuffer; }
			}

			public StringBuilder CommandBuffer {
				get { return _commandBuffer; }
			}
			#endregion
		}
		#endregion

		#region TokenType
		enum TokenType {
			NewStatement = 0,
			Start = 1,
			Statement = 1,
			StartComment = 3,
			Comment = 4,
			PreEndComment = 5,
			StartAnsi = 6,
			EndLineComment = 7,
			String = 8,
			StringQuote = 9,
			SqlString = 10,
			SqlStringQuote = 11,
			StatementQuote = 12,
			FirstSemicolonOnLineSeen = 13,
			PotentialEndFound = 14
		}
		#endregion

		#region RemoveCommentsProperty
		class RemoveCommentsProperty : BooleanPropertyHolder {
			#region ctor
			public RemoveCommentsProperty(SqlStatementSeparator sep)
				: base(sep.removeComments) {
				this.sep = sep;
			}
			#endregion

			#region Fields
			private SqlStatementSeparator sep;
			#endregion

			#region Properties
			public override string DefaultValue {
				get { return "on"; }
			}

			public override string ShortDescription {
				get { return "switches the removal of SQL-comments"; }
			}

			public override string LongDescription {
				get {
					String dsc;
					dsc = "\tSwitch the behaviour to remove all comments\n"
						+ "\tfound in the string sent to the database. Some databases\n"
						+ "\tcan not handle comments in Strings.\n\nValues\n"

						+ "\ttrue\n"
						+ "\t\tDEFAULT. Remove all SQL92 comments found in the given\n"
						+ "\t\tSQL Strings before sending them to the database.\n\n"

						+ "\tfalse\n"
						+ "\t\tSwitch off the default behaviour to remove all\n"
						+ "\t\tcomments found in the string sent to the database.\n"
						+ "\t\tUsually, this is not necessary, but there are\n"
						+ "\t\tconditions where comments actually convey a meaning\n"
						+ "\t\tto the database. For instance hinting in oracle works\n"
						+ "\t\twith comments, like\n"
						+ "\t\t   select /*+ index(foo,foo_fk_idx) */ ....\n"
						+ "\t\t..so removing of comments should be off in this case";
					return dsc;
				}
			}
			#endregion

			#region Public Methods
			public override void OnBooleanPropertyChanged(bool value) {
				sep.RemoveComments(value);
			}
			#endregion
		}
		#endregion
	}
}