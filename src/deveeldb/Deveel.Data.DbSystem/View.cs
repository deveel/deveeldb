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
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

using Deveel.Data.Query;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// A definition of a view stored in the database.
	/// </summary>
	/// <remarks>
	/// It is an object that can be easily serialized and deserialized to/from 
	/// the system view table. It contains the <see cref="DataTableInfo"/>
	/// that describes the characteristics of the view result, and a
	/// <see cref="IQueryPlanNode"/> that describes 
	/// how the view can be constructed.
	/// </remarks>
	public sealed class View {
		/// <summary>
		/// The <see cref="DataTableInfo"/> object that describes the view.
		/// </summary>
		private readonly DataTableInfo viewInfo;

		/// <summary>
		/// The <see cref="IQueryPlanNode"/> that is used to evaluate the view.
		/// </summary>
		private readonly IQueryPlanNode queryNode;

		///<summary>
		///</summary>
		///<param name="viewInfo"></param>
		///<param name="queryNode"></param>
		public View(DataTableInfo viewInfo, IQueryPlanNode queryNode) {
			this.viewInfo = viewInfo;
			this.queryNode = queryNode;
		}

		///<summary>
		/// Returns the <see cref="DataTableInfo"/> object describing the 
		/// structure of this view.
		///</summary>
		public DataTableInfo TableInfo {
			get { return viewInfo; }
		}

		///<summary>
		/// Returns the <see cref="IQueryPlanNode"/> for this view.
		///</summary>
		///<exception cref="Exception"></exception>
		public IQueryPlanNode QueryPlanNode {
			get {
				try {
					return (IQueryPlanNode) queryNode.Clone();
				} catch (Exception e) {
					throw new Exception("Clone error: " + e.Message);
				}
			}
		}

		/// <summary>
		/// Forms this View object into a serialized ByteLongObject object 
		/// that can be stored in a table.
		/// </summary>
		/// <returns></returns>
		internal ByteLongObject SerializeToBlob() {
			try {
				MemoryStream byte_out = new MemoryStream();
				BinaryWriter output = new BinaryWriter(byte_out, Encoding.Unicode);
				// Write the version number
				output.Write(1);
				// Write the DataTableInfo
				TableInfo.Write(output);
				// Serialize the IQueryPlanNode
				BinaryFormatter formatter = new BinaryFormatter();
				MemoryStream obj_stream = new MemoryStream();
				formatter.Serialize(obj_stream, QueryPlanNode);
				obj_stream.Flush();
				byte[] buf = obj_stream.ToArray();
				output.Write(buf.Length);
				output.Write(buf, 0, buf.Length);
				output.Flush();

				return new ByteLongObject(byte_out.ToArray());

			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}

		}

		/// <summary>
		/// Creates an instance of View from the serialized information 
		/// stored in the blob.
		/// </summary>
		/// <param name="blob"></param>
		/// <returns></returns>
		internal static View DeserializeFromBlob(IBlobAccessor blob) {
			Stream blobIn = blob.GetInputStream();
			try {
				BinaryReader input = new BinaryReader(blobIn, Encoding.Unicode);
				// Read the version
				int version = input.ReadInt32();
				if (version != 1)
					throw new IOException("Newer View version serialization: " + version);

				DataTableInfo viewInfo = DataTableInfo.Read(input);
				viewInfo.IsReadOnly = true;

				int length = input.ReadInt32();
				byte[] buf = new byte[length];
				input.Read(buf, 0, length);

				MemoryStream objStream = new MemoryStream(buf);
				BinaryFormatter formatter = new BinaryFormatter();
				formatter.Binder = new ViewBinder();
				IQueryPlanNode viewPlan = (IQueryPlanNode)formatter.Deserialize(objStream);
				objStream.Close();
				return new View(viewInfo, viewPlan);
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			} catch (TypeLoadException e) {
				throw new ApplicationException("Class not found: " + e.Message);
			}
		}

		private class ViewBinder : SerializationBinder {
			public override Type BindToType(string assemblyName, string typeName) {
				Assembly currAssembly = Assembly.GetAssembly(typeof (View));
				string name = currAssembly.GetName().Name;
				if (assemblyName.StartsWith(name)) {
					assemblyName = currAssembly.GetName().FullName;
				}

				return Type.GetType(typeName + ", " + assemblyName, true, true);
			}
		}
	}
}