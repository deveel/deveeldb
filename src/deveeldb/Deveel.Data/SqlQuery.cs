// 
//  Copyright 2010-2014 Deveel
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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

using Deveel.Data.Sql;

namespace Deveel.Data {
	[Serializable]
	public sealed class SqlQuery : ICloneable, ISerializable {
		public SqlQuery(string commandText) {
			if (String.IsNullOrEmpty(commandText))
				throw new ArgumentNullException("commandText");

			Text = commandText;
			Parameters = new ParametersCollection();
		}

		private SqlQuery(SerializationInfo info, StreamingContext context) {
			Text = info.GetString("Text");

			var parameters = (SqlQueryParameter[]) info.GetValue("Parameters", typeof (SqlQueryParameter[]));

			Parameters = new ParametersCollection();
			foreach (var parameter in parameters) {
				Parameters.Add(parameter);
			}
		}

		public string Text { get; private set; }

		public ICollection<SqlQueryParameter> Parameters { get; private set; }

		public object Clone() {
			var query = new SqlQuery(Text);
			foreach (var parameter in Parameters) {
				query.Parameters.Add(parameter);
			}
			return query;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Text", Text);
			info.AddValue("Parameters", Parameters.ToArray());
		}

		public override string ToString() {
			var sb = new StringBuilder();
			sb.Append("[");
			sb.Append(Text);
			sb.Append("]");

			if (Parameters.Count > 0) {
				var count = Parameters.Count;

				sb.Append("[");

				int i = 0;
				foreach (var parameter in Parameters) {
					sb.Append(parameter);

					if (++i < count - 1)
						sb.Append(", ");
				}

				sb.Append("]");
			}

			return sb.ToString();
		}

		public ByteLongObject ToBinary() {
			using (var stream = new MemoryStream()) {
				using (var output = new BinaryWriter(stream, Encoding.Unicode)) {
					try {
						WriteTo(output);
						output.Flush();
						return new ByteLongObject(stream.ToArray());
					} catch (IOException e) {
						throw new ApplicationException("IO Error: " + e.Message);
					}
				}
			}
		}

		public static SqlQuery FromBinary(ByteLongObject binary) {
			using (var stream = new MemoryStream(binary.ToArray())) {
				using (var input = new BinaryReader(stream, Encoding.Unicode)) {
					try {
						return ReadFrom(input);
					} catch (IOException e) {
						throw new ApplicationException("IO Error: " + e.Message);
					}
				}
			}
		}

		public void WriteTo(BinaryWriter output) {
			int textLength = Text.Length;
			var chars = Text.ToCharArray();
			output.Write(textLength);
			output.Write(chars);

			int paramCount = Parameters.Count;
			output.Write(paramCount);

			foreach (var parameter in Parameters) {
				int nameLength = parameter.Name.Length;
				var nameChars = parameter.Name.ToCharArray();
				var sqlType = parameter.SqlType;
				var dir = parameter.Direction;
				var size = parameter.Size;

				output.Write(nameLength);
				output.Write(nameChars);
				output.Write((byte)sqlType);
				output.Write((byte)dir);
				output.Write(size);

				ObjectTransfer.WriteTo(output, parameter.Value);
			}

			output.Flush();
		}

		public static SqlQuery ReadFrom(BinaryReader input) {
			int textLength = input.ReadInt32();
			var textChars = input.ReadChars(textLength);

			var query = new SqlQuery(new string(textChars));

			int paramCount = input.ReadInt32();
			for (int i = 0; i < paramCount; i++) {
				var nameLength = input.ReadInt32();
				var nameChars = input.ReadChars(nameLength);

				var sqlType = (SqlType) input.ReadByte();
				var dir = (ParameterDirection) input.ReadByte();
				var size = input.ReadInt32();

				object value = ObjectTransfer.ReadFrom(input);

				var parameter = new SqlQueryParameter(new string(nameChars), value) {
					SqlType = sqlType,
					Direction = dir, 
					Size = size
				};

				query.Parameters.Add(parameter);
			}

			return query;
		}

		#region ParemetersCollection

		class ParametersCollection : Collection<SqlQueryParameter> {
			private bool markerFound;

			private void ValidateParameter(SqlQueryParameter parameter) {
				if (parameter.IsMarker) {
					markerFound = true;
				} else if (markerFound) {
					throw new ArgumentException("Mixed parameter styles in the same query are not allowed.");
				}

				if (!parameter.IsMarker && 
					Items.Any(x => x.Name == parameter.Name))
					throw new ArgumentException(String.Format("The named parameter {0} was already added to the collection.",
						parameter.Name));
			}

			protected override void InsertItem(int index, SqlQueryParameter item) {
				ValidateParameter(item);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, SqlQueryParameter item) {
				ValidateParameter(item);
				base.SetItem(index, item);
			}
		}

		#endregion
	}
}