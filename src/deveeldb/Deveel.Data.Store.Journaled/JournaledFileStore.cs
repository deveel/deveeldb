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

namespace Deveel.Data.Store.Journaled {
	public sealed class JournaledFileStore : StoreBase {
		private readonly LoggingBufferManager bufferManager;
		private IJournaledResource resource;

		internal JournaledFileStore(string resourceName, LoggingBufferManager bufferManager, bool readOnly) 
			: base(readOnly) {
			this.bufferManager = bufferManager;
			resource = bufferManager.CreateResource(resourceName);
		}

		protected override long DataAreaEndOffset {
			get { return resource.Size; }
		}

		public void Delete() {
			resource.Delete();
		}

		public bool Exists() {
			return resource.Exists;
		}

		protected override void SetDataAreaSize(long length) {
			resource.SetSize(length);
		}

		public override void Lock() {
			bufferManager.Lock();
		}

		public override void Unlock() {
			bufferManager.Unlock();
		}

		protected override void OpenStore(bool readOnly) {
			resource.Open(readOnly);
		}

		protected override void CloseStore() {
			bufferManager.Close(resource);
		}

		protected override int Read(long offset, byte[] buffer, int index, int length) {
			return bufferManager.ReadFrom(resource, offset, buffer, index, length);
		}

		protected override void Write(long offset, byte[] buffer, int index, int length) {
			bufferManager.WriteTo(resource, offset, buffer, index, length);
		}

		protected override void Dispose(bool disposing) {

			base.Dispose(disposing);
		}
	}
}
