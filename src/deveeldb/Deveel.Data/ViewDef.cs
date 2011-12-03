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
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Deveel.Data {
	/// <summary>
	/// A definition of a view stored in the database.
	/// </summary>
	/// <remarks>
	/// It is an object that can be easily serialized and deserialized to/from 
	/// the system view table. It contains the <see cref="Data.DataTableDef"/>
	/// that describes the characteristics of the view result, and a
	/// <see cref="IQueryPlanNode"/> that describes 
	/// how the view can be constructed.
	/// </remarks>
	public class ViewDef {

		/// <summary>
		/// The <see cref="DataTableDef"/> object that describes the view column def.
		/// </summary>
		private readonly DataTableDef view_def;

		/// <summary>
		/// The <see cref="IQueryPlanNode"/> that is used to evaluate the view.
		/// </summary>
		private readonly IQueryPlanNode view_query_node;

		///<summary>
		///</summary>
		///<param name="view_def"></param>
		///<param name="query_node"></param>
		public ViewDef(DataTableDef view_def, IQueryPlanNode query_node) {
			this.view_def = view_def;
			this.view_query_node = query_node;
		}

		///<summary>
		/// Returns the DataTableDef for this view.
		///</summary>
		public DataTableDef DataTableDef {
			get { return view_def; }
		}

		///<summary>
		/// Returns the <see cref="IQueryPlanNode"/> for this view.
		///</summary>
		///<exception cref="Exception"></exception>
		public IQueryPlanNode QueryPlanNode {
			get {
				try {
					return (IQueryPlanNode) view_query_node.Clone();
				} catch (Exception e) {
					throw new Exception("Clone error: " + e.Message);
				}
			}
		}

		/// <summary>
		/// Forms this ViewDef object into a serialized ByteLongObject object 
		/// that can be stored in a table.
		/// </summary>
		/// <returns></returns>
		internal ByteLongObject SerializeToBlob() {
			try {
				MemoryStream byte_out = new MemoryStream();
				BinaryWriter output = new BinaryWriter(byte_out, Encoding.Unicode);
				// Write the version number
				output.Write(1);
				// Write the DataTableDef
				DataTableDef.Write(output);
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
		/// Creates an instance of ViewDef from the serialized information 
		/// stored in the blob.
		/// </summary>
		/// <param name="blob"></param>
		/// <returns></returns>
		internal static ViewDef DeserializeFromBlob(IBlobAccessor blob) {
			Stream blob_in = blob.GetInputStream();
			try {
				BinaryReader input = new BinaryReader(blob_in, Encoding.Unicode);
				// Read the version
				int version = input.ReadInt32();
				if (version == 1) {
					DataTableDef view_def = DataTableDef.Read(input);
					view_def.SetImmutable();
					int length = input.ReadInt32();
					byte[] buf = new byte[length];
					input.Read(buf, 0, length);
					MemoryStream obj_stream = new MemoryStream(buf);

					BinaryFormatter formatter = new BinaryFormatter();
					formatter.Binder = new ViewBinder();
					IQueryPlanNode view_plan = (IQueryPlanNode)formatter.Deserialize(obj_stream);
					obj_stream.Close();
					return new ViewDef(view_def, view_plan);
				} else {
					throw new IOException("Newer ViewDef version serialization: " + version);
				}

			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			} catch (TypeLoadException e) {
				throw new ApplicationException("Class not found: " + e.Message);
			}
		}

		private class ViewBinder : SerializationBinder {
			public override Type BindToType(string assemblyName, string typeName) {
				Assembly currAssembly = Assembly.GetAssembly(typeof (ViewDef));
				string name = currAssembly.GetName().Name;
				if (assemblyName.StartsWith(name)) {
					assemblyName = currAssembly.GetName().FullName;
				}

				return Type.GetType(typeName + ", " + assemblyName, true, true);
			}
		}
	}
}