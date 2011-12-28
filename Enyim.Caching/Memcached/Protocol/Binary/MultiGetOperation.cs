using System;
using System.Linq;
using System.Collections.Generic;

namespace Enyim.Caching.Memcached.Protocol.Binary
{
	public class MultiGetOperation : BinaryMultiItemOperation, IMultiGetOperation, IAsyncOperation
	{
		private static readonly Enyim.Caching.ILog log = Enyim.Caching.LogManager.GetLogger(typeof(MultiGetOperation));

		private Dictionary<string, CacheItem> result;
		private Dictionary<int, string> idToKey;
		private int noopId;

		public MultiGetOperation(IList<string> keys) : base(keys) { }

		protected override BinaryRequest Build(string key)
		{
			var request = new BinaryRequest(OpCode.GetQ)
			{
				Key = key
			};

			return request;
		}

		IList<ArraySegment<byte>> IAsyncOperation.GetBuffer()
		{
			var keys = this.Keys;

			if (keys == null || keys.Count == 0)
			{
				if (log.IsWarnEnabled) log.Warn("Empty multiget!");

				return new ArraySegment<byte>[0];
			}

			if (log.IsDebugEnabled)
				log.DebugFormat("Building multi-get for {0} keys", keys.Count);

			// map the command's correlationId to the item key,
			// so we can use GetQ (which only returns the item data)
			this.idToKey = new Dictionary<int, string>();

			// get ops have 2 segments, header + key
			var buffers = new List<ArraySegment<byte>>(keys.Count * 2);

			foreach (var key in keys)
			{
				var request = this.Build(key);

				request.CreateBuffer(buffers);

				// we use this to map the responses to the keys
				idToKey[request.CorrelationId] = key;
			}

			// uncork the server
			var noop = new BinaryRequest(OpCode.NoOp);
			this.noopId = noop.CorrelationId;

			noop.CreateBuffer(buffers);

			return buffers;
		}

		protected internal override bool ReadResponse(PooledSocket socket)
		{
			this.result = new Dictionary<string, CacheItem>();
			this.Cas = new Dictionary<string, ulong>();

			var response = new BinaryResponse();

			while (response.Read(socket))
			{
				// found the noop, quit
				if (response.CorrelationId == this.noopId)
					return true;

				ProcessResponse(response);
			}

			// finished reading but we did not find the NOOP
			return false;
		}

		class AsyncState
		{
			public AsyncResult<bool> AsyncResult { get; set; }
			public AsyncCallback Callback { get; set; }
			public PooledSocket Socket { get; set; }
			public BinaryResponse Response { get; set; }
		}

		IAsyncResult IAsyncOperation.BeginReadResponse(PooledSocket socket, AsyncCallback callback, object state)
		{
			log.Debug("MultiGetOperation.BeginReadResponse");

			var asyncResult = new AsyncResult<bool>(state);

			this.result = new Dictionary<string, CacheItem>();
			this.Cas = new Dictionary<string, ulong>();

			var response = new BinaryResponse();

			response.BeginRead(socket, EndReadPart, new AsyncState { AsyncResult = asyncResult, Callback = callback, Socket = socket, Response = response });

			return asyncResult;
		}

		private void EndReadPart(IAsyncResult ar)
		{
			var success = false;

			var state = (AsyncState)ar.AsyncState;

			try
			{
				
				if (!state.Response.EndRead(ar) || state.Response.CorrelationId == this.noopId)
				{
					log.Debug("MultiGetOperation.EndReadPart: no more responses to read.");

					success = state.Response.CorrelationId == this.noopId;
				}
				else
				{
					log.Debug("MultiGetOperation.EndReadPart: read next response.");
					ProcessResponse(state.Response);

					state.Response.BeginRead(state.Socket, EndReadPart, state);
					return;
				}			  
			}
			catch (Exception e)
			{
				log.Error(e.Message, e);
				success = false;
			}

			state.AsyncResult.SetComplete(success);

			if (state.Callback != null)
			{
				state.Callback(state.AsyncResult);
			}
		}

		bool IAsyncOperation.EndReadResponse(IAsyncResult ar)
		{
			log.Debug("MultiGetOperation.EndReadResponse: " + ((AsyncResult<bool>)ar).Result);
			return ((AsyncResult<bool>) ar).Result;
		}

		private void ProcessResponse(BinaryResponse response)
		{
			string key;

			// find the key to the response
			if (!this.idToKey.TryGetValue(response.CorrelationId, out key))
			{
				// we're not supposed to get here tho
				log.WarnFormat("Found response with CorrelationId {0}, but no key is matching it.", response.CorrelationId);
				return;
			}

			if (log.IsDebugEnabled) log.DebugFormat("Reading item {0}", key);

			// deserialize the response
			int flags = BinaryConverter.DecodeInt32(response.Extra, 0);

			this.result[key] = new CacheItem((ushort)flags, response.Data);
			this.Cas[key] = response.CAS;
		}

		public Dictionary<string, CacheItem> Result
		{
			get { return this.result; }
		}

		Dictionary<string, CacheItem> IMultiGetOperation.Result
		{
			get { return this.result; }
		}
	}
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion
