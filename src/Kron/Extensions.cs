#region Copyright (c) 2016 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#endregion

namespace Kron
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    static class FuncExtensions
    {
        public static Action<T> Ignore<T, TResult>(this Func<T, TResult> func)
        {
            if (func == null) throw new ArgumentNullException(nameof(func));
            return arg => func(arg);
        }
    }

    static class Extensions
    {
        public static Func<TArgs, Task> CreateEventAsyncRaiser<TSender, TArgs>(
            this TSender sender,
            Func<TSender, EventHandler<TArgs>> handlerSelector,
            CancellationToken cancellationToken,
            TaskScheduler scheduler)
        {
            if (sender == null) throw new ArgumentNullException(nameof(sender));
            if (handlerSelector == null) throw new ArgumentNullException(nameof(handlerSelector));

            return args =>
            {
                var handler = handlerSelector(sender);
                if (handler == null)
                    return Task.FromResult(0);
                var task = new Task(() =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    handler(sender, args);
                });
                task.Start(scheduler);
                return task;
            };
        }
    }
}