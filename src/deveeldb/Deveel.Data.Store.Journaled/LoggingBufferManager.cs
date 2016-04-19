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
using System.Collections.Generic;
using System.IO;
using System.Threading;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;

namespace Deveel.Data.Store.Journaled {
	public sealed class LoggingBufferManager : IDisposable {
		private int currentTime;
		private readonly object timeLock = new object();

		private Page[] pageMap = new Page[257];
		private List<Page> pages = new List<Page>();
		private int currentPageCount = 0;
		private readonly PageComparer pageComparer;

		private readonly object writeLock = new object();
		private int writeLockCount = 0;
		private bool checkpointInProgress;

		private int uniqueId;

		internal LoggingBufferManager(IContext context, IFileSystem fileSystem, string journalPath, IStoreDataFactory dataFactory, int pageSize, int maxPages, bool readOnly) {
			Context = context;

			pageComparer = new PageComparer(this);

			JournalPath = journalPath;
			FileSystem = fileSystem;
			PageSize = pageSize;
			MaxPages = maxPages;
			ReadOnly = readOnly;

			JournaledSystem = new JournaledSystem(context, FileSystem, JournalPath, ReadOnly, true, PageSize, dataFactory);
		}

		~LoggingBufferManager() {
			Dispose(false);
		}

		private JournaledSystem JournaledSystem { get; set; }

		public IContext Context { get; private set; }

		public int MaxPages { get; private set; }

		public int PageSize { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public string JournalPath { get; private set; }

		public bool ReadOnly { get; private set; }

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

		private void OnPageAccessed(Page page) {
			lock (timeLock) {
				page.Time = currentTime;
				++currentTime;
				page.Access();
			}
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
				while (page != null && !page.Matches(id, pageNumber)) {
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

		public void Start() {
			JournaledSystem.Start();
		}

		public void Stop() {
			JournaledSystem.Stop();
		}

		public IJournaledResource CreateResource(string resourceName) {
			return JournaledSystem.CreateResource(resourceName);
		}

		public void Lock() {
			lock (writeLock) {
				while (checkpointInProgress) {
					Monitor.Wait(writeLock);
				}
				++writeLockCount;
			}
		}

		public void Unlock() {
			lock (writeLock) {
				--writeLockCount;
				Monitor.PulseAll(writeLock);
			}
		}

		public void SetCheckPoint(bool flushJournals) {
			// Wait until the writes have finished
			lock (writeLock) {
				while (writeLockCount > 0) {
					Monitor.Wait(writeLock);
				}
				checkpointInProgress = true;
			}

			try {
				Context.OnDebug("Checkpoint requested");

				lock (pageMap) {
					// Flush all the pages out to the log.
					for (int i = 0; i < pageMap.Length; ++i) {
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

				JournaledSystem.SetCheckPoint(flushJournals);
			} finally {
				// Make sure we unset the check point in progress flag and notify
				// any blockers.
				lock (writeLock) {
					checkpointInProgress = false;
					Monitor.PulseAll(writeLock);
				}
			}
		}

		internal int ReadFrom(IJournaledResource data, long position, byte[] buffer, int offset, int count) {
			int origLen = count;
			long pageNumber = position / PageSize;
			int startOffset = (int)(position % PageSize);
			int toRead = System.Math.Min(count, PageSize - startOffset);

			var page = FetchPage(data, pageNumber);
			lock (page) {
				try {
					page.Open();
					page.Read(startOffset, buffer, offset, toRead);
				} finally {
					page.Dispose();
				}
			}

			count -= toRead;

			while (count > 0) {
				offset += toRead;
				position += toRead;
				++pageNumber;
				toRead = System.Math.Min(count, PageSize);

				page = FetchPage(data, pageNumber);
				lock (page) {
					try {
						page.Open();
						page.Read(0, buffer, offset, toRead);
					} finally {
						page.Dispose();
					}
				}
				count -= toRead;
			}

			return origLen;
		}

		internal void WriteTo(IJournaledResource data, long position, byte[] buf, int off, int len) {
			long pageNumber = position / PageSize;
			int startOffset = (int)(position % PageSize);
			int toWrite = System.Math.Min(len, PageSize - startOffset);

			var page = FetchPage(data, pageNumber);
			lock (page) {
				try {
					page.Open();
					page.Write(startOffset, buf, off, toWrite);
				} finally {
					page.Dispose();
				}
			}

			len -= toWrite;

			while (len > 0) {
				off += toWrite;
				position += toWrite;
				++pageNumber;
				toWrite = System.Math.Min(len, PageSize);

				page = FetchPage(data, pageNumber);
				lock (page) {
					try {
						page.Open();
						page.Write(0, buf, off, toWrite);
					} finally {
						page.Dispose();
					}
				}

				len -= toWrite;
			}
		}

		internal void Close(IJournaledResource data) {
			long id = data.Id;
			// Flush all changes made to the resource then close.
			lock (pageMap) {
				// Flush all the pages out to the log.
				// This scans the entire hash for values and could be an expensive
				// operation.  Fortunately 'close' isn't used all that often.
				for (int i = 0; i < pageMap.Length; ++i) {
					var page = pageMap[i];
					Page prev = null;

					while (page != null) {
						bool deletedHash = false;
						if (page.Id == id) {
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
						}

						// Go to next page in hash chain
						if (!deletedHash) {
							prev = page;
						}

						page = page.Next;
					}
				}
			}

			data.Close();
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (JournaledSystem != null)
					JournaledSystem.Dispose();
			}

			JournaledSystem = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		#region Page

		class Page : IDisposable {
			private int refCount;

			private bool initd;
			private byte[] pageBuffer;
			private int firstWriteOffset;
			private int lastWriteOffset;

			public Page(IJournaledResource data, long number, int size) {
				Data = data;
				Number = number;
				Size = size;

				refCount = 0;
				Reset();
			}

			~Page() {
				Dispose(false);
			}

			public long Number { get; private set; }

			public int Size { get; private set; }

			public IJournaledResource Data { get; private set; }

			public Page Next { get; set; }

			public bool IsUsed {
				get { return refCount > 0; }
			}

			public long Id {
				get { return Data.Id; }
			}

			public int AccessCount { get; private set; }

			public int Time { get; set; }

			public void Reset() {
				if (refCount != 0)
					throw new InvalidOperationException("Cannot reset a page while still referenced.");

				initd = false;
				Time = 0;
				AccessCount = 0;
			}

			public void Reference() {
				refCount++;
			}

			public void Dereference() {
				if (refCount <= 0)
					throw new InvalidOperationException("Cannot dereference a non referenced page.");

				refCount--;
			}

			public void Access() {
				AccessCount++;
			}

			public bool Matches(long id, long page) {
				return Id == id &&
				       Number == page;
			}

			private void ReadContent(long pageNumber, byte[] buffer, int offset) {
				if (offset != 0)
					throw new ArgumentException("The offset must be 0", "offset");

				Data.Read(pageNumber, buffer, offset);
			}

			public void Flush() {
				if (!initd)
					return;

				if (lastWriteOffset > -1) {
					Data.Write(Number, pageBuffer, firstWriteOffset, lastWriteOffset - firstWriteOffset);
				}

				firstWriteOffset = Int32.MaxValue;
				lastWriteOffset = -1;
			}

			public void Open() {
				if (!initd) {
					// Create the buffer to contain the page in memory
					pageBuffer = new byte[Size];
					// Read the page.  This will either read the page from the backing
					// store or from a log.
					ReadContent(Number, pageBuffer, 0);

					initd = true;
					firstWriteOffset = Int32.MaxValue;
					lastWriteOffset = -1;
				}
			}

			public void Read(int pageOffset, byte[] buffer, int offset, int count) {
				Array.Copy(pageBuffer, pageOffset, buffer, offset, count);
			}

			public void Write(int pageOffset, byte[] buffer, int offset, int count) {
				firstWriteOffset = System.Math.Min(pageOffset, firstWriteOffset);
				lastWriteOffset = System.Math.Max(pageOffset + count, lastWriteOffset);


				Array.Copy(buffer, offset, pageBuffer, pageOffset, count);
			}

			public override bool Equals(object obj) {
				var other = obj as Page;
				if (other == null)
					return false;

				return Id == other.Id &&
				       Number == other.Number;
			}

			public override int GetHashCode() {
				return (int) unchecked (Number ^ Id);
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					Dereference();

					if (refCount == 0) {
						if (initd) {
							Flush();
							initd = false;
						}

						pageBuffer = null;
					}
				}
			}
		}

		#endregion

		#region PageComparer

		class PageComparer : IComparer<Page> {
			private readonly LoggingBufferManager bufferManager;

			public PageComparer(LoggingBufferManager bufferManager) {
				this.bufferManager = bufferManager;
			}

			/// <summary>
			/// The calculation for finding the <i>weight</i> of a page in the cache.
			/// </summary>
			/// <param name="page"></param>
			/// <remarks>
			/// A heavier page is sorted lower and is therefore cleared from the 
			/// cache faster.
			/// </remarks>
			/// <returns></returns>
			private float Weight(Page page) {
				// We fix the access counter so it can not exceed 10000 accesses.  I'm
				// a little unsure if we should WriteByte this constant in the equation but it
				// ensures that some old but highly accessed page will not stay in the
				// cache forever.
				long boundedPageCount = System.Math.Min(page.AccessCount, 10000);
				float v = (1f / boundedPageCount) * (bufferManager.currentTime - page.Time);
				return v;
			}


			public int Compare(Page x, Page y) {
				var w1 = Weight(x);
				var w2 = Weight(y);

				return w1.CompareTo(w2);
			}
		}

		#endregion
	}
}
