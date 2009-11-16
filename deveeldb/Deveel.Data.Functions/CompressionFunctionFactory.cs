using System;
using System.IO;
using System.Text;

using Deveel.Zip;

namespace Deveel.Data.Functions {
	internal class CompressionFunctionFactory : FunctionFactory {
		public override void Init() {
			AddFunction("crc32", typeof(Crc32Function));
			AddFunction("adler32", typeof (Adler32Function));
			AddFunction("compress", typeof(CompressFunction));
			AddFunction("uncompress", typeof(UncompressFunction));
		}

		private static void ReadIntoStream(TextReader reader, Stream stream) {
			string line;
			while ((line = reader.ReadLine()) != null) {
				byte[] buffer = Encoding.Unicode.GetBytes(line);
				stream.Write(buffer, 0, buffer.Length);
			}
		}

		private static void CopyStream(Stream input, Stream output) {
			const int bufferSize = 1024;
			byte[] buffer = new byte[bufferSize];
			int readCount;
			while ((readCount = input.Read(buffer, 0, bufferSize)) != 0) {
				output.Write(buffer, 0, readCount);
			}
		}

		#region Crc32Function

		[Serializable]
		private class Crc32Function : Function {
			public Crc32Function(Expression[] parameters)
				: base("crc32", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				//TODO: needs some revision...
				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor) ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(TType.StringType);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				byte[] result = stream.ToArray();
				CRC32 crc32 = new CRC32();
				crc32.update(result);

				return TObject.GetBigNumber(crc32.getValue());
			}

		}

		#endregion

		#region Adler32Function

		[Serializable]
		private class Adler32Function : Function {
			public Adler32Function(Expression[] parameters) 
				: base("adler32", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				//TODO: needs some revision...
				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(TType.StringType);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				Adler32 adler32 = new Adler32();
				adler32.update(stream.ToArray());
				return TObject.GetBigNumber(adler32.getValue());
			}
		}

		#endregion

		#region CompressFunction

		[Serializable]
		private class CompressFunction : Function {
			public CompressFunction(Expression[] parameters) 
				: base("compress", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(TType.StringType);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}

				MemoryStream tempStream = new MemoryStream();
				DeflaterOutputStream outputStream = new DeflaterOutputStream(tempStream);

				const int bufferSize = 1024;
				byte[] buffer = new byte[bufferSize];

				int bytesRead;
				while ((bytesRead = stream.Read(buffer, 0, bufferSize)) != 0) {
					outputStream.Write(buffer, 0, bytesRead);
				}

				outputStream.Flush();

				byte[] result = tempStream.ToArray();
				return new TObject(TType.BinaryType, result);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.BinaryType;
			}
		}

		#endregion

		#region UncompressFunction

		[Serializable]
		private class UncompressFunction : Function {
			public UncompressFunction(Expression[] parameters) 
				: base("uncompress", parameters) {
			}

			public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
				TObject ob = this[0].Evaluate(group, resolver, context);
				if (ob.IsNull)
					return TObject.Null;

				MemoryStream stream;
				if (ob.TType is TStringType) {
					IStringAccessor str = (IStringAccessor)ob.Object;
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				} else if (ob.TType is TBinaryType) {
					IBlobAccessor blob = (IBlobAccessor)ob.Object;
					stream = new MemoryStream(blob.Length);
					CopyStream(blob.GetInputStream(), stream);
				} else {
					ob = ob.CastTo(TType.StringType);
					StringObject str = StringObject.FromString(ob.ToStringValue());
					TextReader reader = str.GetTextReader();
					stream = new MemoryStream(str.Length);
					ReadIntoStream(reader, stream);
				}
				
				MemoryStream tmpStream = new MemoryStream();
				InflaterInputStream inputStream = new InflaterInputStream(stream);

				const int bufferSize = 1024;
				byte[] buffer = new byte[bufferSize];

				int bytesRead;
				while ((bytesRead = inputStream.Read(buffer, 0, bufferSize)) != 0) {
					tmpStream.Write(buffer, 0, bytesRead);
				}

				byte[] output = tmpStream.ToArray();
				return new TObject(TType.BinaryType, output);
			}

			public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
				return TType.BinaryType;
			}
		}

		#endregion
	}
}