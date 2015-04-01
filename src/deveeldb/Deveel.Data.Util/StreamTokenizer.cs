using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Util {
	/// <summary>
	///     Read any Stream using a TextReader and move from token to token
	///     First setup the syntax by specifying the word, number, special and whitespace characters
	///     Then, repeat to call NextToken() to read each token until NextToken() returns TokenType.EOF
	///     Once a token has been read, you can access it through the NVal (numeric) and SVal (string) properties
	/// </summary>
	internal class StreamTokenizer {
		#region Asserts

		/// <summary>
		///     Reads the next token and tests its type against the provided (expected) type
		///     Throws an AssertException in case the types doesn't match!
		/// </summary>
		/// <param name="type">Expected type of next token</param>
		/// <param name="peek">If true, the read token is pushed back again (peek-mode)</param>
		public void AssertToken(int type, bool peek = false) {
			// Peek at next token and push it back again (peek)
			var token = NextToken();
			if (peek)
				PushBackToken();

			// Test token type and throw excpetion
			if (token != type)
				throw new AssertException(string.Format("Assert token type '{0}' failed! Read token '{1}'.", type, token));
		}

		#endregion

		#region Buffer-Filling

		/// <summary>
		///     Reads from the given TextReader to the char buffer used to tokenize.
		///     Always refills the buffer with the next bytes read from the TextReader.
		///     Adds only that much bytes to the buffer as the TextReader returns with one read operation!
		/// </summary>
		/// <returns>Returns true if the buffer position isn't at the end of the buffer</returns>
		private bool RefillReadBuffer() {
			// If buffer needs some refilling, do it!
			if (buffer.Length < MaxBufferSize || bufPos >= RefillBufferPos) {
				// Remaining unused bytes in buffer
				var remain = buffer.Length - bufPos;

				// Bytes to read from input stream and to refill buffer with
				var toRead = MaxBufferSize - remain;
				var buf = new char[toRead];

				// Read from input buffer
				var read = reader.Read(buf, 0, toRead);
				if (read > 0) {
					// If something was read, merge the remaining buffer with the new read bytes
					var fin = new char[(toRead == read) ? MaxBufferSize : (remain + read)];
					if (remain > 0)
						Array.Copy(buffer, bufPos, fin, 0, remain);

					Array.Copy(buf, 0, fin, remain, read);
					bufPos = 0;
					buffer = fin;
				}
			}

			// Return false, if the buffer position is at the end of the buffer
			return !(bufPos > buffer.Length - 1);
		}

		#endregion

		#region Char-Type

		/// <summary>
		///     Returns the CharType for a given character code
		///     if no CharType is specified, the char code is return instead
		/// </summary>
		/// <param name="chr">Character code to get the CharType for</param>
		/// <returns>Returns CharType or char code, if no CharType is found</returns>
		private int GetCharType(int chr) {
			if (CharTypes.ContainsKey(chr))
				return (int) CharTypes[chr];

			return chr;
		}

		#endregion

		#region AssertException

		/// <summary>
		///     Exception throw if token type assertion fails
		/// </summary>
		public class AssertException : Exception {
			/// <summary>
			///     Initializes a new instance of the <see cref="AssertException" /> class
			/// </summary>
			/// <param name="s">message text</param>
			public AssertException(string s)
				: base(s) {
			}
		}

		#endregion

		#region PushbackType

		/// <summary>
		///     Stores type, SVal and NVal of a pushed back token
		/// </summary>
		public class PushedBackToken {
			/// <summary>
			///     Gets the Token type of a pushed back token
			/// </summary>
			public int Type { get; set; }

			/// <summary>
			///     Gets the string value of a pushed back token
			/// </summary>
			public string SVal { get; set; }

			/// <summary>
			///     Gets the number value of a pushed back token
			/// </summary>
			public double NVal { get; set; }
		}

		#endregion

		#region constants

		/// <summary>
		///     Used buffer size used to read the stream in bytes
		/// </summary>
		private const int MaxBufferSize = 2048;

		/// <summary>
		///     As soon as this position is reached within the buffer, the buffer is refilled
		///     This ensures that a token is completely read before it is parsed
		/// </summary>
		private const int RefillBufferPos = 1024;

		#endregion

		#region Private Vars

		/// <summary>
		///     TextReader used to read the input stream
		/// </summary>
		private readonly TextReader reader;

		/// <summary>
		///     Current position in buffer
		/// </summary>
		private int bufPos;

		/// <summary>
		///     Character buffer filled from input stream
		/// </summary>
		private char[] buffer;

		/// <summary>
		///     NewLine flag
		///     Used to skip repeating NewLine tokens (like in \r\n, only one NewLine token will be returned)
		/// </summary>
		private bool newLine;

		/// <summary>
		///     Last char parsed
		///     Used to skip repeating NewLine tokens (like in \r\n, only one NewLine token will be returned)
		/// </summary>
		private int lastChar;

		/// <summary>
		///     Used for EolIsSignificant Property
		/// </summary>
		private bool eolIsSignificant;

		#endregion

		#region Contructors

		/// <summary>
		///     Initializes a new instance of the <see cref="StreamTokenizer" /> class
		///     Creates a new StreamTokenizer, which helps to parse tokens from an input stream using a TextReader
		/// </summary>
		/// <param name="reader">TextReader to use as input for all the tokenizing work</param>
		public StreamTokenizer(TextReader reader)
			: this() {
			this.reader = reader;
		}

		/// <summary>
		///     Prevents a default instance of the <see cref="StreamTokenizer" /> class from being created
		///     Private constructor used by public constructors to setup default values
		/// </summary>
		private StreamTokenizer() {
			Type = (int) TokenType.Undef;
			PushedBackTokens = new Stack<PushedBackToken>();
			LowerCaseMode = false;

			LineNo = 1;
			buffer = new char[0];

			ResetSyntax();
		}

		#endregion

		#region enums

		/// <summary>
		///     These are the token types supported
		/// </summary>
		public enum TokenType {
			/// <summary>
			///     No token found. Initial token type.
			/// </summary>
			Undef = -4,

			/// <summary>
			///     End of stream reached
			/// </summary>
			EOF = -5,

			/// <summary>
			///     End of line reached
			///     This token type is only used if EolIsSignificant is set to true
			/// </summary>
			EOL = -3,

			/// <summary>
			///     The token is a number (access it through NVal)
			///     Based on the setup char types the token is a number token (numeric value)
			/// </summary>
			Number = -2,

			/// <summary>
			///     The token is a word (access it through SVal)
			///     Based on the setup char types, the token is a word token (string value)
			/// </summary>
			Word = -1
		}

		/// <summary>
		///     These are the types of characters you may use to define the syntax
		///     Each char (code: 0x00 - 0xFF) will have its own char type
		/// </summary>
		public enum CharType : byte {
			Ordinary,
			Whitespace,
			Word,
			Number,
			Quote,
			NewLine,
			Comment
		}

		#endregion

		#region Token-Properties

		/// <summary>
		///     Gets the current token number value
		/// </summary>
		public double NVal { get; private set; }

		/// <summary>
		///     Gets the current token string value
		/// </summary>
		public string SVal { get; private set; }

		/// <summary>
		///     Gets the current token type
		///     <para>
		///         After a call to the NextToken() method, this field contains the type of the token just read.
		///         For a single character token, its value is the single character, converted to an integer.
		///         For a quoted string token, its value is the quote character.
		///         Otherwise, its value is one of the following:
		///         • TokenType.WORD indicates that the token is a word
		///         • TokenType.NUMBER indicates that the token is a number
		///         • TokenType.EOL indicates that the end of line has been read. The field can only have this value if
		///         EolIsSignificant has been to true.
		///         • TokenType.EOF indicates that the end of the input stream has been reached.
		///         • TokenType.UNDEF initial value.
		///     </para>
		/// </summary>
		public int Type { get; private set; }

		/// <summary>
		///     Gets the stack of pushed back tokens used to keep track of all pushed back tokens
		/// </summary>
		public Stack<PushedBackToken> PushedBackTokens { get; private set; }

		/// <summary>
		///     Gets the line number within input stream (only useable if NewLine chars are setup!)
		/// </summary>
		public int LineNo { get; private set; }

		#endregion

		#region Syntax-Properties

		/// <summary>
		///     Gets or sets a value indicating whether or not ends of lines are treated as EOL tokens
		/// </summary>
		public bool EolIsSignificant {
			get { return eolIsSignificant; }

			set {
				eolIsSignificant = value;
				if (value) {
					CharTypes[10] = CharType.NewLine; // \r
					CharTypes[13] = CharType.NewLine; // \n
				} else {
					CharTypes[10] = CharType.Whitespace; // \r
					CharTypes[13] = CharType.Whitespace; // \n
				}
			}
		}

		/// <summary>
		///     Gets or sets a value indicating whether or not word token are automatically lowercased
		/// </summary>
		public bool LowerCaseMode { get; set; }

		/// <summary>
		///     Gets a list all possible character codes (0x00 - 0xFF) with the associated CharType
		/// </summary>
		public Dictionary<int, CharType> CharTypes { get; private set; }

		#endregion

		#region Syntax-Methods

		#region Single Chars

		/// <summary>
		///     Sets a character to be treated as ordinary char
		/// </summary>
		/// <param name="code">the char value</param>
		public void SetOrdinaryChar(int code) {
			if (CharTypes.ContainsKey(code))
				CharTypes[code] = CharType.Ordinary;
			else
				CharTypes.Add(code, CharType.Ordinary);
		}

		/// <summary>
		///     Sets a character to be treated as whitespace char
		/// </summary>
		/// <param name="code">the char value</param>
		public void SetWhitespaceChar(int code) {
			if (CharTypes.ContainsKey(code))
				CharTypes[code] = CharType.Whitespace;
			else
				CharTypes.Add(code, CharType.Whitespace);
		}

		/// <summary>
		///     Sets a character to be treated as word char
		/// </summary>
		/// <param name="code">the char value</param>
		public void SetWordChar(int code) {
			if (CharTypes.ContainsKey(code))
				CharTypes[code] = CharType.Word;
			else
				CharTypes.Add(code, CharType.Word);
		}

		/// <summary>
		///     Sets a character to be treated as number char
		/// </summary>
		/// <param name="code">the char value</param>
		public void SetNumberChar(int code) {
			if (CharTypes.ContainsKey(code))
				CharTypes[code] = CharType.Number;
			else
				CharTypes.Add(code, CharType.Number);
		}

		/// <summary>
		///     Sets a character to be treated as quote char
		/// </summary>
		/// <param name="code">the char value</param>
		public void SetQuoteChar(int code) {
			if (CharTypes.ContainsKey(code))
				CharTypes[code] = CharType.Quote;
			else
				CharTypes.Add(code, CharType.Quote);
		}

		public void SetCommentChar(int code) {
			if (CharTypes.ContainsKey(code))
				CharTypes[code] = CharType.Comment;
			else
				CharTypes.Add(code, CharType.Comment);
		}

		#endregion

		#region Char Regions

		/// <summary>
		///     Sets a range of characters to be treated as ordinary chars
		/// </summary>
		/// <param name="from">range start value</param>
		/// <param name="to">range end value</param>
		public void SetOrdinaryChars(int from, int to) {
			if (to < from)
				throw new ArgumentException("Character range error");

			for (var c = from; c <= to; c++)
				SetOrdinaryChar(c);
		}

		/// <summary>
		///     Sets a range of characters to be treated as whitespace chars
		/// </summary>
		/// <param name="from">range start value</param>
		/// <param name="to">range end value</param>
		public void SetWhitespaceChars(int from, int to) {
			if (to < from)
				throw new ArgumentException("Character range error");

			for (var c = from; c <= to; c++)
				SetWhitespaceChar(c);
		}

		/// <summary>
		///     Sets a range of characters to be treated as word chars
		/// </summary>
		/// <param name="from">range start value</param>
		/// <param name="to">range end value</param>
		public void SetWordChars(int from, int to) {
			if (to < from)
				throw new ArgumentException("Character range error");

			for (var c = from; c <= to; c++)
				SetWordChar(c);
		}

		/// <summary>
		///     Sets a range of characters to be treated as number chars
		/// </summary>
		/// <param name="from">range start value</param>
		/// <param name="to">range end value</param>
		public void SetNumberChars(int from, int to) {
			if (to < from)
				throw new ArgumentException("Character range error");

			for (var c = from; c <= to; c++)
				SetNumberChar(c);
		}

		/// <summary>
		///     Sets a range of characters to be treated as quote chars
		/// </summary>
		/// <param name="from">range start value</param>
		/// <param name="to">range end value</param>
		public void SetQuoteChars(int from, int to) {
			if (to < from)
				throw new ArgumentException("Character range error");

			for (var c = from; c <= to; c++)
				SetQuoteChar(c);
		}

		#endregion

		/// <summary>
		///     Sets all chars from 0x00 to 0xFF to be ordinary
		/// </summary>
		public void ResetSyntax() {
			CharTypes = new Dictionary<int, CharType>();
			for (var c = 0x00; c <= 0xFF; c++)
				SetOrdinaryChar(c);
		}

		/// <summary>
		///     Specifies that numbers should be parsed by this tokenizer
		///     These chars are treated as number chars: 0123456789.-
		/// </summary>
		public void EnableParseNumbers() {
			SetNumberChars('0', '9');
			SetNumberChar('.');
			SetNumberChar('-');
		}

		#endregion

		#region Token returning-Methods

		/// <summary>
		///     Causes the next call to the NextToken() method of this tokenizer
		///     to return the current value in the Type field
		///     and not to modify the values in the NVal or SVal fields
		/// </summary>
		public void PushBackToken() {
			PushedBackTokens.Push(new PushedBackToken {
				NVal = NVal,
				SVal = SVal,
				Type = Type
			});
		}

		/// <summary>
		///     Reads the next token and returns its TokenType
		///     If the last token was pushed back, this token is returned again
		///     If there was a quote the returned value isn't the TokenType!
		///     Its the char code used to start and end the quote
		///     If the token is a ordinary single char token, the char code is returned instead of TokenType.Ordinary
		/// </summary>
		/// <returns>TokenType of current token</returns>
		public int NextToken() {
			// Return pushed back tokens first
			if (PushedBackTokens.Count > 0) {
				// Get latest pushed back token from stack
				var pbtoken = PushedBackTokens.Pop();

				Type = pbtoken.Type;
				SVal = pbtoken.SVal;
				SVal = (LowerCaseMode ? SVal.ToLower() : SVal);
				NVal = pbtoken.NVal;

				return Type;
			}

			// Last token was NewLine => Return NewLine Token now
			if (newLine) {
				// skip in case of chr(13)+chr(10)
				if (this.lastChar == 13 && buffer[bufPos] == 10)
					bufPos++;

				newLine = false;
				LineNo++;
				if (EolIsSignificant) {
					Type = (int) TokenType.EOL;
					SVal = string.Empty;
					NVal = 0;
					return Type;
				}
			}

			// Check and fill read buffer
			if (!RefillReadBuffer()) {
				// end of file reached
				Type = (int) TokenType.EOF;
				SVal = string.Empty;
				NVal = 0;
				return Type;
			}

			//// Setup some variables for the token parsing
			// Char code of active quote char 
			var quoteChar = -1;

			// Keep parsing till true
			var exitLoop = false;

			// Current token builder
			var token = new StringBuilder();

			// Current tokens type. Initial TokenType: Undefined
			var tokentype = (int) TokenType.Undef;

			// Current char read from buffer
			var chr = 0;

			// Current chars type. Initial CharType: Ordinary
			var chrtype = (int) CharType.Ordinary;

			// Remember last character (as we override it soon)
			var lastChar = this.lastChar;

			// Loop through characters from buffer
			while (!exitLoop) {
				// Reached end of buffer and refilling the buffer doesn't work anymore => End reached!
				if (bufPos >= buffer.Length && !RefillReadBuffer()) {
					tokentype = (int) TokenType.EOF;

					// Exit character parsing loop, NOW
					exitLoop = true;
					continue;
				}

				// If still (again) buffer filled take the next char
				chr = buffer[bufPos++];
				this.lastChar = chr;

				// Aquire character type from defined syntax
				chrtype = GetCharType(chr);

				// Switch through all those CharTypes with the current characters type
				switch (chrtype) {
					// NewLine Char
					case (int) CharType.NewLine:

						// If within a quote, append new line to quote text (token)
						if (quoteChar >= 0)
							token.Append((char) chr);
						else {
							// Set current token type to EOL
							if (tokentype == (int) TokenType.Undef)
								tokentype = (int) TokenType.EOL;
							else
								newLine = true;

							exitLoop = true;
						}

						break;

					// Whitespace Char
					case (int) CharType.Whitespace:

						// If within a quote, append char to quote text (token)
						if (quoteChar >= 0)
							token.Append((char) chr);
						else {
							// There is no Whitespace TokenType, so just terminate the current token
							if (tokentype != (int) TokenType.Undef)
								exitLoop = true;
						}

						break;

					// Quote Char
					case (int) CharType.Quote:

						// If not already within a quote, set current TokenType to Word (Quotes are Words!)
						if (quoteChar == -1) {
							tokentype = (int) TokenType.Word;

							// Remember which quote char started the quote
							quoteChar = chr;
						} else {
							// Within a quote 
							// If current quote char isn't the same char that started the qoute, then it simply is part of the quote
							if (quoteChar != chr)
								token.Append((char) chr);
							else {
								// If the quote is ended by the right char, end parsing, as the quote (word) token is terminated, too
								exitLoop = true;
							}
						}

						break;

					// Number Char
					case (int) CharType.Number:

						// Append current token with number char and set TokenType.Number, if no type set jet
						token.Append((char) chr);
						if (tokentype == (int) TokenType.Undef)
							tokentype = (int) TokenType.Number;

						break;

					// Word Char
					case (int) CharType.Word:

						// Append current token with word char and set TokenType.Word, if no type set jet
						token.Append((char) chr);
						if (tokentype == (int) TokenType.Undef)
							tokentype = (int) TokenType.Word;

						break;

					// Ordinary Char
					case (int) CharType.Ordinary:

						// If current token is empty, set this single char ordinary token
						// or if in quote add char anyway
						if (token.Length == 0 || quoteChar != -1)
							token.Append((char) chr);
						else {
							// If there was a token ongoing, terminate it and push back this ordinary token
							PushedBackTokens.Push(new PushedBackToken {
								NVal = 0,
								SVal = ((char) chr).ToString(),
								Type = chr
							});
						}

						// Exit loop, if not in quote
						exitLoop = (quoteChar == -1);
						break;

					// Unkown Char (?!?!)
					default:
						exitLoop = true;
						break;
				}
			}
			//// End of character looping 

			// If there was a quote the returned value isn't the TokenType! 
			// Its the char code used to start and end the quote
			// Token: Quote
			if (quoteChar >= 0) {
				Type = quoteChar;
				SVal = token.ToString();
				SVal = (LowerCaseMode ? SVal.ToLower() : SVal);
				NVal = 0;
				return Type;
			}

			// Token: EOL / Word / Number / Ordinary single char 
			Type = tokentype;
			switch (tokentype) {
				case (int) TokenType.Number:
					SVal = string.Empty;
					NVal = Convert.ToDouble(token.ToString());
					break;

				case (int) TokenType.Word:
					SVal = token.ToString();
					SVal = (LowerCaseMode ? SVal.ToLower() : SVal);
					NVal = 0;
					break;

				case (int) TokenType.Undef:
					SVal = token.ToString();
					SVal = (LowerCaseMode ? SVal.ToLower() : SVal);
					NVal = 0;
					Type = chr; // Single char token (ordinary char) Return char code 
					break;

				case (int) TokenType.EOL:
					SVal = string.Empty;
					NVal = 0;
					break;

				case (int) TokenType.EOF:
					SVal = string.Empty;
					NVal = 0;
					break;

				default:
					break;
			}

			// Retrun the Type specified before
			return Type;
		}

		#endregion
	}
}