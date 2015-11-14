// 
//  Copyright 2010-2015 Deveel
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
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Configuration;

namespace Deveel.Data.Store.Journaled {
	public sealed class BufferManager : IBufferManager, IConfigurable {
		private IComparer<Page> pageComparer;
		private List<Page> pages;
		private Page[] pageMap;
		private int currentPageCount;

		private int currentTime;
		private readonly object timeLock = new object();

		private JournalingSystem journalingSystem;

		private readonly object writeLock = new object();
		private int writeLockCount;
		private bool checkpointInProgress;

		public BufferManager(ISystemContext systemContext) {
			SystemContext = systemContext;

			currentTime = 0;
			pageComparer = new PageComparer(this);

			pages = new List<Page>();
			pageMap = new Page[256];
		}

		public int MaxPages { get; set; }

		public int PageSize { get; set; }

		public ISystemContext SystemContext { get; private set; }

		public string JournalPath { get; set; }

		public bool ReadOnly { get; set; }

		public bool EnableLogging { get; set; }

		public void Dispose() {
			throw new NotImplementedException();
		}

		public void Start() {
			journalingSystem = new JournalingSystem(SystemContext);
			journalingSystem.JournalPath = JournalPath;
			journalingSystem.PageSize = PageSize;
			journalingSystem.ReadOnly = ReadOnly;
			journalingSystem.EnableLogging = EnableLogging;
			journalingSystem.Start();
		}

		public void Stop() {
			if (journalingSystem != null)
				journalingSystem.Stop();
		}

		public IJournaledResource CreateResource(string resourceName) {
			return journalingSystem.CreateResource(resourceName);
		}

		private static int CalcHashCode(long id, long pageNumber) {
			return (int)((id << 6) + (pageNumber * ((id + 25) << 2)));
		}

		private Page FetchPage(IJournaledResource data, long pageNumber) {
			long id = data.Id;

			Page prevPage = null;
			bool newPage = false;
			Page page;

			lock (pageMap) {
				// Generate the hash code for this page.
				int p = (CalcHashCode(id, pageNumber) & 0x07FFFFFFF) % pageMap.Length;

				// Search for this page in the hash
				page = pageMap[p];

				while (page != null && !page.IsPage(id, pageNumber)) {
					prevPage = page;
					page = page.Next;
				}

				// Page isn't found so create it and add to the cache
				if (page == null) {
					page = new Page(data, pageNumber, PageSize);
					// Add this page to the map
					page.Next = pageMap[p];
					pageMap[p] = page;
				} else {
					// Move this page to the head if it's not already at the head.
					if (prevPage != null) {
						prevPage.Next = page.Next;
						page.Next = pageMap[p];
						pageMap[p] = page;
					}
				}

				lock (page) {
					// If page not in use then it must be newly setup, so add a
					// reference.
					if (!page.IsUsed) {
						page.Reset();
						newPage = true;
						page.Reference();
					}

					// Add a reference for this fetch
					page.Reference();
				}

			}

			// If the page is new,
			if (newPage) {
				OnPageCreated(page);
			} else {
				OnPageAccessed(page);
			}

			// Return the page.
			return page;
		}

		private void OnPageAccessed(Page page) {
			lock (timeLock) {
				page.Time = currentTime;
				++currentTime;
				++page.AccessCount;
			}
		}

		private void OnPageCreated(Page page) {
			lock (timeLock) {
				page.Time = currentTime;
				++currentTime;

				++currentPageCount;
				pages.Add(page);

				// Below is the page purge algorithm.  If the maximum number of pages
				// has been created we sort the page list weighting each page by time
				// since last accessed and total number of accesses and clear the bottom
				// 20% of this list.

				// Check if we should purge old pages and purge some if we do...
				if (currentPageCount > MaxPages) {
					// Purge 20% of the cache
					// Sort the pages by the current formula,
					//  ( 1 / page_access_count ) * (current_t - page_t)
					// Further, if the page has written data then we multiply by 0.75.
					// This scales down page writes so they have a better chance of
					// surviving in the cache than page writes.
					var pageArray = pages.ToArray();
					Array.Sort(pageArray, pageComparer);

					int purgeSize = System.Math.Max((int)(pageArray.Length * 0.20f), 2);
					for (int i = 0; i < purgeSize; ++i) {
						var dpage = pageArray[pageArray.Length - (i + 1)];
						lock (dpage) {
							dpage.Dispose();
						}
					}

					// Remove all the elements from page_list and set it with the sorted
					// list (minus the elements we removed).
					pages.Clear();

					for (int i = 0; i < pageArray.Length - purgeSize; ++i) {
						pages.Add(pageArray[i]);
					}

					currentPageCount -= purgeSize;
				}
			}
		}

		public int Read(IJournaledResource data, long position, byte[] buffer, int offset, int length) {
			int origLen = length;
			long pageNumber = position / PageSize;
			int startOffset = (int)(position % PageSize);
			int toRead = System.Math.Min(length, PageSize - startOffset);

			var page = FetchPage(data, pageNumber);
			lock (page) {
				try {
					page.Open();
					page.Read(startOffset, buffer, offset, toRead);
				} finally {
					page.Dispose();
				}
			}

			length -= toRead;
			while (length > 0) {
				offset += toRead;
				position += toRead;
				++pageNumber;
				toRead = System.Math.Min(length, PageSize);

				page = FetchPage(data, pageNumber);
				lock (page) {
					try {
						page.Open();
						page.Read(0, buffer, offset, toRead);
					} finally {
						page.Dispose();
					}
				}

				length -= toRead;
			}

			return origLen;
		}

		public void Write(IJournaledResource data, long position, byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Lock() {
			lock (writeLock) {
				if (checkpointInProgress)
					Monitor.Wait(writeLock);

				writeLockCount++;
			}
		}

		public void Release() {
			lock (writeLock) {
				writeLockCount--;
				Monitor.PulseAll(writeLock);
			}
		}

		public void Checkpoint() {
			lock (writeLock) {
				while (writeLockCount > 0) {
					Monitor.Wait(writeLock);
				}

				checkpointInProgress = true;
			}

			try {
				lock (pageMap) {
					for (int i = 0; i < pageMap.Length; i++) {
						var page = pageMap[i];
						Page prev = null;

						while (page != null) {
							bool deletedHash = false;
							lock (page) {
								// Flush the page (will only actually flush if there are changes)
								page.Flush();

								// Remove this page if it is no longer in use
								if (!page.IsUsed) {
									deletedHash = true;
									if (prev == null) {
										pageMap[i] = page.Next;
									} else {
										prev.Next = page.Next;
									}
								}

							}

							// Go to next page in hash chain
							if (!deletedHash) {
								prev = page;
							}
							page = page.Next;
						}
					}
				}

				journalingSystem.Checkpoint(false);
			} finally {
				lock (writeLock) {
					checkpointInProgress = false;
					Monitor.PulseAll(writeLock);
				}
			}
		}

		void IConfigurable.Configure(IConfiguration config) {
			// TODO:
		}

		#region Page

		class Page : IDisposable, IEquatable<Page> {
			private int refCount;

			private byte[] content;

			private int firstWriteOffset;
			private int lastWriteOffset;

			public Page(IJournaledResource resource, long number, int size) {
				Resource = resource;
				Number = number;
				PageSize = size;

				Reset();
			}

			~Page() {
				Dispose(false);
			}

			public IJournaledResource Resource { get; private set; }

			public long Number { get; private set; }

			public int PageSize { get; private set; }

			public bool IsOpen { get; private set; }

			public Page Next { get; set; }

			public int Time { get; set; }

			public int AccessCount { get; set; }

			public long ResourceId {
				get { return Resource.Id; }
			}

			public bool IsUsed {
				get { return refCount > 0; }
			}

			public void Reset() {
				if (refCount != 0)
					throw new InvalidOperationException("Cannot reset if the page is still referenced.");

				IsOpen = false;
				AccessCount = 0;
				Time = 0;
			}

			public void Reference() {
				refCount++;
			}

			public void Unreference() {
				if (refCount <= 0)
					throw new InvalidOperationException("Too many times unreferenced");

				refCount--;
			}

			private void ReadContent(long pageNumber, byte[] buffer, int offset) {
				if (offset != 0)
					throw new InvalidOperationException();

				Resource.Read(pageNumber, buffer, offset);
			}

			public void Open() {
				if (IsOpen)
					return;

				content = new byte[PageSize];
				ReadContent(Number, content, 0);

				IsOpen = true;

				firstWriteOffset = int.MaxValue;
				lastWriteOffset = -1;
			}

			public void Flush() {
				if (!IsOpen)
					return;

				if (lastWriteOffset > -1) {
					// Write to the store data.
					Resource.Write(Number, content, firstWriteOffset, lastWriteOffset - firstWriteOffset);
				}

				firstWriteOffset = Int32.MaxValue;
				lastWriteOffset = -1;
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					Unreference();

					if (refCount == 0 && IsOpen)
						Flush();

					IsOpen = false;
				}

				content = null;
			}

			public void Read(int offset, byte[] buffer, int index, int length) {
				Array.Copy(content, offset, buffer, index, length);
			}

			public void Write(int offset, byte[] buffer, int index, int length) {
				firstWriteOffset = System.Math.Min(offset, firstWriteOffset);
				lastWriteOffset = System.Math.Max(offset + length, lastWriteOffset);

				Array.Copy(buffer, index, buffer, offset, length);
			}

			public bool IsPage(long id, long number) {
				return ResourceId == id && Number == number;
			}

			public bool Equals(Page other) {
				return ResourceId == other.ResourceId &&
				       Number == other.Number;
			}

			public override bool Equals(object obj) {
				var other = obj as Page;
				return Equals(other);
			}

			public override int GetHashCode() {
				return (int)unchecked (ResourceId^((int)Number));
			}
		}

		#endregion

		#region PageComparer

		class PageComparer : IComparer<Page> {
			private readonly BufferManager bufferManager;

			public PageComparer(BufferManager bufferManager) {
				this.bufferManager = bufferManager;
			}

			private float PageWeight(Page page) {
				// We fix the access counter so it can not exceed 10000 accesses.  I'm
				// a little unsure if we should write this constant in the equation but it
				// ensures that some old but highly accessed page will not stay in the
				// cache forever.
				long boundedPageCount = System.Math.Min(page.AccessCount, 10000);
				return (1f / boundedPageCount) * (bufferManager.currentTime - page.Time);
			}


			public int Compare(Page x, Page y) {
				var weightX = PageWeight(x);
				var weightY = PageWeight(y);
				return weightX.CompareTo(weightY);
			}
		}

		#endregion
	}
}
