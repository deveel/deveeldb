using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
#if PCL
using System.Threading.Tasks;
#endif

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Store.Journaled {
	class JournaledSystem : IDisposable {
		private Dictionary<string, ResourceBase> allResources;
		private long seqId;
		private readonly List<JournalFile> journalArchives;
		private JournalFile topJournalFile;
		private readonly object topJournalLock = new object();
		private long journalNumber;
		private JournalingThread journalingThread;

		private readonly object initLock = new object();

		public JournaledSystem(IContext context, IFileSystem fileSystem, string journalPath, bool readOnly, bool enableLogging, int pageSize, IStoreDataFactory dataFactory) {
			Context = context;
			FileSystem = fileSystem;
			JournalPath = journalPath;
			ReadOnly = readOnly;
			PageSize = pageSize;
			DataFactory = dataFactory;
			EnableLogging = enableLogging;

			allResources = new Dictionary<string, ResourceBase>();
			journalNumber = 0;
			journalArchives = new List<JournalFile>();
		}

		~JournaledSystem() {
			Dispose(false);
		}

		public IContext Context { get; private set; }

		public IStoreDataFactory DataFactory { get; private set; }

		public string JournalPath { get; private set; }

		public IFileSystem FileSystem { get; private set; }

		public int PageSize { get; private set; }

		public bool ReadOnly { get; private set; }

		public bool EnableLogging { get; private set; }

		private JournalFile TopJournal {
			get {
				lock (topJournalLock) {
					return topJournalFile;
				}
			}
		}

		private static String GetJournalFileName(int number) {
			if (number < 10 || number > 73) {
				throw new Exception("Journal file name output of range.");
			}
			return String.Format("jnl{0}", number);
		}

		private void RollForwardRecover() {
			// The list of all journal files,
			var journalFilesList = new List<JournalSummary>();

			// Scan the journal path for any journal files.
			for (int i = 10; i < 74; ++i) {
				string journalFn = GetJournalFileName(i);
				string f = FileSystem.CombinePath(JournalPath, journalFn);
				// If the journal exists, create a summary of the journal
				if (FileSystem.FileExists(f)) {
					if (ReadOnly) {
						throw new IOException(
							"Journal file " + f + " exists for a read-only session.  " +
							"There may not be any pending journals for a Read-only session.");
					}

					var jf = new JournalFile(this, FileSystem, f, ReadOnly);

					// Open the journal file for recovery.  This will set various
					// information about the journal such as the last check point and the
					// id of the journal file.
					JournalSummary summary = jf.OpenForRecovery();
					// If the journal can be recovered from.
					if (summary.CanBeRecovered) {
						Context.OnInformation(String.Format("Journal '{0}' found: can be recovered", jf));

						journalFilesList.Add(summary);
					} else {
						Context.OnInformation(String.Format("Deleting journal '{0}': nothing to recover", jf));

						// Otherwise close and delete it
						jf.CloseAndDelete();
					}
				}
			}

			// Sort the journal file list from oldest to newest.  The oldest journals
			// are recovered first.
			journalFilesList.Sort(new JournalSummaryComparer());

			long lastJournalNumber = -1;

			// Persist the journals
			for (int i = 0; i < journalFilesList.Count; ++i) {
				var summary = journalFilesList[i];

				// Check the resources for this summary
				var resList = summary.Resources;
				foreach (string resourceName in resList) {
					// This puts the resource into the hash map.
					CreateResource(resourceName);
				}

				// Assert that we are recovering the journals input the correct order
				JournalFile jf = summary.JournalFile;
				if (jf.JournalNumber < lastJournalNumber) {
					throw new InvalidOperationException("Assertion failed, sort failed.");
				}

				lastJournalNumber = jf.JournalNumber;

				Context.OnInformation(String.Format("Recovering '{0}' (8 .. {1})", jf, summary.LastCheckPoint));

				jf.Persist(8, summary.LastCheckPoint);
				// Then close and delete.
				jf.CloseAndDelete();

				// Check the resources for this summary and close them
				foreach (var resourceName in resList) {
					var resource = (ResourceBase) CreateResource(resourceName);
					// When we finished, make sure the resource is closed again
					// Close the resource
					resource.PersistClose();
					// Post recover notification
					resource.OnPostRecover();
				}
			}
		}

		private void NewTopJournalFile() {
			var journalFn = GetJournalFileName((int)((journalNumber & 63) + 10));
			++journalNumber;

			string f = FileSystem.CombinePath(JournalPath, journalFn);
			if (FileSystem.FileExists(f)) {
				throw new IOException("Journal file already exists.");
			}

			topJournalFile = new JournalFile(this, FileSystem, f, ReadOnly);
			topJournalFile.Open(journalNumber - 1);
		}

		internal JournalEntry LogPageModification(string resourceName, long pageNumber, byte[] buf, int off, int len) {
			lock (topJournalLock) {
				return TopJournal.LogPageModification(resourceName, pageNumber, buf, off, len);
			}
		}

		internal void LogResourceSizeChange(string resourceName, long newSize) {
			lock (topJournalLock) {
				TopJournal.LogResourceSizeChange(resourceName, newSize);
			}
		}

		internal void LogResourceDelete(string resourceName) {
			lock (topJournalLock) {
				TopJournal.LogResourceDelete(resourceName);
			}
		}

		public void Start() {
			if (EnableLogging) {
				lock (initLock) {
					if (journalingThread == null) {
						// Start the background journaling thread,
						journalingThread = new JournalingThread(this);
						journalingThread.Start();
						// Scan for any changes and make the changes.
						RollForwardRecover();

						if (!ReadOnly) {
							// Create a new top journal file
							NewTopJournalFile();
						}
					} else {
						throw new Exception("Assertion failed - already started.");
					}
				}
			}
		}

		public void Stop() {
			if (EnableLogging) {
				lock (initLock) {
					if (journalingThread != null) {
						// Stop the journal thread
						journalingThread.PersistArchives(0);
						journalingThread.Finish();
						journalingThread.Wait();
						journalingThread = null;
					} else {
						throw new Exception("Assertion failed - already stopped.");
					}
				}

				if (!ReadOnly) {
					// Close any remaining journals and roll forward recover (shouldn't
					// actually be necessary but just incase...)
					lock (topJournalLock) {
						// Close all the journals
						int sz = journalArchives.Count;
						for (int i = 0; i < sz; ++i) {
							var jf = journalArchives[i];
							jf.Close();
						}

						// Close the top journal
						TopJournal.Close();
						// Scan for journals and make the changes.
						RollForwardRecover();

						DisposeResources();
					}
				}
			}
		}

		private void DisposeResources() {
			if (allResources != null) {
				foreach (var resource in allResources.Values) {
					if (resource != null)
						resource.Dispose();
				}

				allResources.Clear();
			}
		}

		public IJournaledResource CreateResource(string resourceName) {
			ResourceBase resource;
			lock (allResources) {
				// Has this resource previously been open?
				if (!allResources.TryGetValue(resourceName, out resource)) {
					// No...
					// Create a unique id for this
					long id = seqId;
					++seqId;

					// Create the IStoreDataAccessor for this resource.
					var accessor = DataFactory.CreateData(resourceName);
					if (EnableLogging) {
						resource = new LoggingResource(this, id, resourceName, accessor);
					} else {
						resource = new NonLoggingResource(this, id, resourceName, accessor);
					}
					// Put this input the map.
					allResources[resourceName] = resource;
				}
			}

			// Return the resource
			return resource;
		}

		internal ResourceBase GetResource(string resourceName) {
			lock (allResources) {
				ResourceBase resource;
				if (!allResources.TryGetValue(resourceName, out resource))
					return null;

				return resource;
			}
		}

		public void SetCheckPoint(bool flushJournals) {
			// No Logging
			if (!EnableLogging) {
				return;
			}
			// Return if Read-only
			if (ReadOnly) {
				return;
			}

			bool persisting;

			lock (topJournalLock) {
				JournalFile topJ = TopJournal;

				// When the journal exceeds a threshold then we cycle the top journal
				if (flushJournals || topJ.Length > (256 * 1024)) {
					// Cycle to the next journal file
					NewTopJournalFile();
					// Add this to the archives
					journalArchives.Add(topJ);
				}

				persisting = journalArchives.Count > 0;
				topJ.SetCheckPoint();
			}

			if (persisting) {
				// Notifies the background thread that there is something to persist.
				// This will block until there are at most 10 journal files open.
				journalingThread.PersistArchives(10);
			}
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (journalingThread != null)
					Stop();
			}

			allResources = null;
			FileSystem = null;
			Context = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}


		#region JounralingThread

		class JournalingThread {
			private JournaledSystem system;
#if PCL
			private Task task;
#else
			private Thread thread;
#endif

			private bool finished = false;
			private bool actually_finished;

			public JournalingThread(JournaledSystem system) {
				this.system = system;

#if PCL
				task = new Task(Execute);
#else
				thread = new Thread(Execute);
				thread.Name = "JournalingThread";
				thread.IsBackground = true;
#endif
			}

			private void Execute() {
				bool localFinished = false;

				while (!localFinished) {
					List<JournalFile> toProcess = null;
					lock (system.topJournalLock) {
						if (system.journalArchives.Count > 0) {
							toProcess = new List<JournalFile>();
							toProcess.AddRange(system.journalArchives);
						}
					}

					if (toProcess == null) {
						// Nothing to process so wait
						lock (this) {
							if (!finished) {
#if PCL
								try {
									Monitor.Wait(this);
								} catch (OperationCanceledException) {
								}
#else
								try {
									Monitor.Wait(this);
								} catch (ThreadInterruptedException e) { /* ignore */ }
#endif
							}
						}

					} else if (toProcess.Count > 0) {
						// Something to process, so go ahead and process the journals,
						int sz = toProcess.Count;
						// For all journals
						for (int i = 0; i < sz; ++i) {
							// Pick the lowest journal to persist
							JournalFile jf = toProcess[i];
							try {
								// Persist the journal
								jf.Persist(8, jf.Length);
								// Close and then delete the journal file
								jf.CloseAndDelete();
							} catch (IOException e) {
								system.Context.OnError(String.Format("Error persisting journal '{0}", jf), e);

								// If there is an error persisting the best thing to do is
								// finish
								lock (this) {
									finished = true;
								}
							}
						}
					}

					lock (this) {
						localFinished = finished;
						// Remove the journals that we have just persisted.
						if (toProcess != null) {
							lock (system.topJournalLock) {
								int sz = toProcess.Count;
								for (int i = 0; i < sz; ++i) {
									system.journalArchives.RemoveAt(0);
								}
							}
						}

						// Notify any threads waiting
						Monitor.PulseAll(this);
					}
				}

				lock (this) {
					actually_finished = true;
					Monitor.PulseAll(this);
				}
			}

			public void Start() {
				lock (this) {
#if PCL
					task.Start();
#else
					thread.Start();
#endif
				}
			}

			public void PersistArchives(int untilSize) {
				lock (this) {
					Monitor.PulseAll(this);
					int sz;
					lock (system.topJournalLock) {
						sz = system.journalArchives.Count;
					}
					// Wait until the sz is smaller than 'until_size'
					while (sz > untilSize) {
#if PCL
						try {
							Monitor.Wait(this);
						} catch (OperationCanceledException) {
							/* ignore */
						}

#else
						try {
							Monitor.Wait(this);
						} catch (ThreadInterruptedException e) {
							/* ignore */
						}
#endif
						lock (system.topJournalLock) {
							sz = system.journalArchives.Count;
						}
					}
				}
			}

			public void Finish() {
				lock (this) {
					finished = true;
					Monitor.PulseAll(this);
				}
			}

			public void Wait() {
				lock (this) {
#if PCL
					try {
						while (!actually_finished) {
							Monitor.Wait(this);
						}
					} catch (OperationCanceledException e) {
						throw new Exception("Canceled: " + e.Message);
					}
					Monitor.PulseAll(this);
				}
#else
					try {
						while (!actually_finished) {
							Monitor.Wait(this);
						}
					} catch (ThreadInterruptedException e) {
						throw new Exception("Interrupted: " + e.Message);
					}
					Monitor.PulseAll(this);
				}
#endif
			}
		}

#endregion

#region JournalSummaryComparer

		private class JournalSummaryComparer : IComparer<JournalSummary> {
			public int Compare(JournalSummary x, JournalSummary y) {
				long jn1 = x.JournalFile.JournalNumber;
				long jn2 = y.JournalFile.JournalNumber;

				return jn1.CompareTo(jn2);
			}
		}

#endregion
	}
}
