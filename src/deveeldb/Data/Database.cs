using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Deveel.Data.Configurations;
using Deveel.Data.Events;
using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Storage;
using Deveel.Data.Transactions;

using IsolationLevel = Deveel.Data.Transactions.IsolationLevel;

namespace Deveel.Data {
	public sealed class Database : Context, IDatabase {
		private readonly OpenTransactionCollection transactions;
		private IStoreSystem storeSystem;

		public Database(IDatabaseSystem system, string name, IConfiguration configuration) 
			: base(system, KnownScopes.Database) {
			System = system;
			Name = name;
			Configuration = configuration;

			Locker = new Locker(this);

			Scope.AsContainer().RegisterInstance<IDatabase>(this);
			Scope.AsContainer().RegisterInstance<IStoreSystem>(StoreSystem);

			TableSystem = Scope.Resolve<ITableSystem>();

			transactions = new OpenTransactionCollection();
		}

		IEventSource IEventSource.ParentSource => System;

		IDictionary<string, object> IEventSource.Metadata => GetMetadata();

		public string Name { get; }

		public IDatabaseSystem System { get; }

		public Locker Locker { get; }

		public Version Version { get; }

		public bool Exists { get; }

		public DatabaseStatus Status { get; private set; }

		public IStoreSystem StoreSystem => DiscoverStoreSystem();

		public ITransactionCollection OpenTransactions => transactions;

		public ITableSystem TableSystem { get; }

		private IStoreSystem DiscoverStoreSystem() {
			if (storeSystem == null) {
				var typeString = Configuration.GetString("store.type");

				if (String.IsNullOrEmpty(typeString)) {
					storeSystem = Scope.Resolve<IStoreSystem>();
				} else {
					var type = Type.GetType(typeString, false);
					if (type == null || !typeof(IStoreSystem).IsAssignableFrom(type))
						throw new InvalidOperationException($"Type '{typeString}' is not valid for a store system");

					storeSystem = Scope.Resolve(type) as IStoreSystem;
				}
			}

			return storeSystem;
		}

		private IDictionary<string, object> GetMetadata() {
			return new Dictionary<string, object>();
		}

		private IEnumerable<IDatabaseFeature> GetAllFeatures(IEnumerable<IDatabaseFeature> features) {
			var result = new List<IDatabaseFeature>();
			result.AddRange(Scope.ResolveAll<IDatabaseFeature>());

			if (features != null) {
				foreach (var feature in features) {
					var featureType = feature.GetType();
					if (!result.Any(x => featureType.IsInstanceOfType(x)))
						result.Add(feature);
				}
			}


			return result;
		}

		public void Create(IEnumerable<IDatabaseFeature> features) {
			try {
				features = GetAllFeatures(features);

				TableSystem.Create();

				using (var session = this.CreateSystemSession("sys")) {
					try {
						foreach (var feature in features) {
							// TODO: configure the System Schema
						}

						session.Transaction.Commit(null);
					} catch (Exception ex) {
						throw new DataException("An error occurred while setting up one of the database features", ex);
					} finally {
						TableSystem.Close();
					}
				}
			} catch (Exception ex) {
				throw new DatabaseException("Could not create the database", ex);
			}
		}

		public void Open() {
			throw new NotImplementedException();
		}

		public void Close() {
			throw new NotImplementedException();
		}

		public void Shutdown() {
			throw new NotImplementedException();
		}

		ITransaction IDatabase.CreateTransaction(IsolationLevel isolationLevel) {
			if (isolationLevel == IsolationLevel.Unspecified)
				isolationLevel = IsolationLevel.Serializable;

			if (isolationLevel != IsolationLevel.Serializable)
				throw new NotSupportedException();


			return CreateTransaction();
		}

		bool IDatabase.CloseTransaction(ITransaction transaction) {
			lock (this) {
				return transactions.RemoveTransaction(transaction);
			}
		}

		public Transaction CreateTransaction() {
			lock (this) {
				var visibleTables = TableSystem.GetTableSources().ToArray();
				var indexes = visibleTables.Select(x => x.CreateRowIndexSet()).ToArray();
				var commitId = OpenTransactions.CurrentCommitId;

				var transaction = new Transaction(this, TableSystem, commitId, visibleTables, indexes);
				transactions.AddTransaction(transaction);

				return transaction;
			}
		}


		public IConfiguration Configuration { get; }

		#region OpenTransactionCollection

		class OpenTransactionCollection : ITransactionCollection {
			private readonly List<ITransaction> transactions;
			private long minCommitId;
			private long maxCommitId;

			public OpenTransactionCollection() {
				transactions = new List<ITransaction>();
				minCommitId = Int64.MaxValue;
				maxCommitId = 0;
			}

			public int Count {
				get {
					lock (this) {
						return transactions.Count;
					}
				}
			}

			public long CurrentCommitId { get; private set; }

			public void AddTransaction(ITransaction transaction) {
				lock (this) {
					long currentCommitId = transaction.CommitId;

					if (currentCommitId < maxCommitId)
						throw new InvalidOperationException("Added a transaction with a lower than maximum commit id");

					transactions.Add(transaction);

					//TODO: SystemContext.Stats.Increment(StatsDefaultKeys.OpenTransactionsCount);
					maxCommitId = currentCommitId;
				}
			}

			public void Clear() {
				lock (this) {
					transactions.Clear();
				}
			}

			public bool RemoveTransaction(ITransaction transaction) {
				lock (this) {
					int size = transactions.Count;
					int i = transactions.IndexOf(transaction);

					if (i == 0) {
						// First in list.
						if (i == size - 1) {
							// And last.
							minCommitId = Int32.MaxValue;
							maxCommitId = 0;
						} else {
							minCommitId = transactions[i + 1].CommitId;
						}
					} else if (i == transactions.Count - 1) {
						// Last in list.
						maxCommitId = transactions[i - 1].CommitId;
					} else if (i == -1) {
						return false;
					}

					transactions.RemoveAt(i);
					CurrentCommitId++;

					//TODO: SystemContext.Stats.Decrement(StatsDefaultKeys.OpenTransactionsCount);
					return true;
				}
			}

			public long MinimumCommitId(ITransaction transaction) {
				lock (this) {
					long commitId = Int64.MaxValue;

					if (transactions.Count > 0) {
						// If the bottom transaction is this transaction, then go to the
						// next up from the bottom (we don't count this transaction as the
						// minimum commit_id).
						var testTransaction = transactions[0];

						if (testTransaction != transaction) {
							commitId = testTransaction.CommitId;
						} else if (transactions.Count > 1) {
							commitId = transactions[1].CommitId;
						}
					}

					return commitId;
				}
			}

			public IEnumerator<ITransaction> GetEnumerator() {
				lock (this) {
					return transactions.GetEnumerator();
				}
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public ITransaction FindById(long commitId) {
				lock (this) {
					return transactions.FirstOrDefault(x => x.CommitId == commitId);
				}
			}
		}

		#endregion
	}
}