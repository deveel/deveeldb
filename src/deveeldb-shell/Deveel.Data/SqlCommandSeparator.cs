//
//  Copyright 2011 Deveel
//
//  This file is part of DeveelDBShell.
//
//  DeveelDBShell is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  DeveelDBShell is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with DeveelDBShell. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections;
using System.Text;

using Deveel.Console.Commands;

namespace Deveel.Data {
	public sealed class SqlCommandSeparator : ICommandSeparator {
		private bool removeComments;
		private ParseState currentState;
		private readonly Stack stateStack;

		public SqlCommandSeparator() {
			stateStack = new Stack();
			currentState = new ParseState();
		}

		private void ParsePartialInput() {
			int pos = 0;
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

			while (state != TokenType.PotentialEndFound && pos < input.Length) {
				bool vetoAppend = false;
				bool reIterate;
				char current = input[pos];
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

		public void Dispose() {
		}

		public bool MoveNext() {
			if (currentState.Type == TokenType.PotentialEndFound)
				throw new InvalidOperationException("call Cont() or Consumed() before MoveNext()");
			if (currentState.InputBuffer.Length == 0)
				return false;

			ParsePartialInput();
			return (currentState.Type == TokenType.PotentialEndFound);
		}

		public void Reset() {
			currentState.InputBuffer.Length = 0;
			currentState.CommandBuffer.Length = 0;
			currentState.Type = TokenType.NewStatement;
		}

		public string Current {
			get {
				if (currentState.Type != TokenType.PotentialEndFound)
					throw new InvalidOperationException("Current called without MoveNext()");

				return currentState.CommandBuffer.ToString();
			}
		}

		object IEnumerator.Current {
			get { return Current; }
		}

		public void Append(string line) {
			currentState.InputBuffer.Append(line);
		}

		public void Push() {
			stateStack.Push(currentState);
			currentState = new ParseState();
		}

		public void Pop() {
			currentState = (ParseState)stateStack.Pop();
		}

		public void Cont() {
			currentState.Type = TokenType.Start;
		}

		public void Consumed() {
			currentState.Type = TokenType.NewStatement;
		}

		#region ParseState
		class ParseState {
			private TokenType state;
			private readonly StringBuilder inputBuffer;
			private readonly StringBuilder commandBuffer;
			/*
			 * instead of adding new states, we store the
			 * fact, that the last 'potential_end_found' was
			 * a newline here.
			 */
			private bool eolineSeen;

			public ParseState() {
				eolineSeen = true; // we start with a new line.
				state = TokenType.NewStatement;
				inputBuffer = new StringBuilder();
				commandBuffer = new StringBuilder();
			}


			public TokenType Type {
				get { return state; }
				set { state = value; }
			}

			public bool NewlineSeen {
				get { return eolineSeen; }
				set { eolineSeen = value; }
			}

			public StringBuilder InputBuffer {
				get { return inputBuffer; }
			}

			public StringBuilder CommandBuffer {
				get { return commandBuffer; }
			}
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
	}
}