// 
//  Copyright 2010-2016 Deveel
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data.Sql.Objects {
	public sealed class SqlLongString : ISqlString, IObjectRef, IDisposable {
		public static readonly SqlLongString Null = new SqlLongString(null, null, true);

		private SqlLongString(ILargeObject largeObject, Encoding encoding, bool isNull) {
			if (!isNull && largeObject == null)
				throw new ArgumentNullException("largeObject");

			LargeObject = largeObject;
			Encoding = encoding;
			IsNull = isNull;

			if (!isNull) {
				Length = encoding.GetMaxCharCount((int)largeObject.RawSize);
			}
		}

		public SqlLongString(ILargeObject largeObject, Encoding encoding)
			: this(largeObject, encoding, false) {
		}

		~SqlLongString() {
			Dispose(false);
		}

		public Encoding Encoding { get; private set; }

		long IObjectRef.Size {
			get { return LargeObject.RawSize; }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (LargeObject != null)
					LargeObject.Dispose();
			}

			Encoding = null;
			LargeObject = null;
		}

		int IComparable.CompareTo(object obj) {
			throw new NotSupportedException();
		}

		int IComparable<ISqlObject>.CompareTo(ISqlObject other) {
			throw new NotSupportedException();
		}

		public ObjectId ObjectId {
			get { return LargeObject.Id; }
		}

		internal  ILargeObject LargeObject { get; private set; }

		public bool IsNull { get; private set; }

		bool ISqlObject.IsComparableTo(ISqlObject other) {
			return false;
		}

		public char this[long offset] {
			get {
				if (offset > Length)
					throw new ArgumentOutOfRangeException("offset");
				if (LargeObject == null)
					return '\0';

				throw new NotImplementedException();
			}
		}

		int IComparable<ISqlString>.CompareTo(ISqlString other) {
			throw new NotSupportedException();
		}

		public IEnumerator<char> GetEnumerator() {
			return new Enumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public long Length { get; private set; }

		public TextReader GetInput(Encoding encoding) {
			if (LargeObject == null)
				return TextReader.Null;

			return new StreamReader(new ObjectStream(LargeObject), encoding);
		}

		public static SqlLongString Unicode(ILargeObject largeObject) {
			return new SqlLongString(largeObject, Encoding.Unicode);
		}

#if !PCL
		public static SqlLongString Ascii(ILargeObject largeObject) {
			return new SqlLongString(largeObject, Encoding.ASCII);
		}
#endif

		public static SqlLongString Create(IRequest context, Stream inputStream, Encoding encoding, bool compressed = true) {
			if (context == null)
				throw new ArgumentNullException("context");
			if (inputStream == null)
				throw new ArgumentNullException("inputStream");
			if (!inputStream.CanRead)
				throw new ArgumentException("The input stream is not readable", "inputStream");

			var maxSize = inputStream.Length;
			var lob = context.Query.Session.CreateLargeObject(maxSize, compressed);
			using (var stream = new ObjectStream(lob)) {
				inputStream.CopyTo(stream, 1024);
				stream.Flush();
			}

			lob.Complete();
			return new SqlLongString(lob, encoding);
		}

		public static SqlLongString Unicode(IRequest context, Stream inputStream, bool compressed = true) {
			return Create(context, inputStream, Encoding.Unicode, compressed);
		}

#if !PCL
		public static SqlLongString Ascii(IRequest context, Stream inputStream, bool compressed = true) {
			return Create(context, inputStream, Encoding.ASCII, compressed);
		}
#endif

		public static SqlLongString Create(IRequest context, string text, Encoding encoding, bool compressed = true) {
			var bytes = encoding.GetBytes(text);
			var stream = new MemoryStream(bytes);
			return Create(context, stream, encoding, compressed);
		}

		public static SqlLongString Unicode(IRequest context, string text, bool compressed = true) {
			return Create(context, text, Encoding.Unicode, compressed);
		}

#if !PCL
		public static SqlLongString Ascii(IRequest context, string text) {
			return Create(context, text, Encoding.ASCII);
		}
#endif

		#region Enumerator

		class Enumerator : IEnumerator<char> {
			private TextReader reader;
			private readonly SqlLongString longString;
			private int curChar;

			public Enumerator(SqlLongString longString) {
				this.longString = longString;
				reader = longString.GetInput(longString.Encoding);
			}

			public void Dispose() {
				if (reader != null) {
					reader.Dispose();
					reader = null;
				}
			}

			public bool MoveNext() {
				curChar = reader.Read();
				return curChar != -1;
			}

			public void Reset() {
				reader = longString.GetInput(longString.Encoding);
			}

			public char Current {
				get {
					if (curChar == -1)
						throw new EndOfStreamException();

					return (char) curChar;
				}
			}

			object IEnumerator.Current {
				get { return Current; }
			}
		}

		#endregion
	}
}