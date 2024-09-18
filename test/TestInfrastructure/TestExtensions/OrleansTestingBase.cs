using System;
using Orleans.Runtime;

using SafeRandom = System.Random;

namespace TestExtensions
{
    public abstract class OrleansTestingBase
    {
        private static readonly SafeRandom safeRandom = new SafeRandom();
        protected static readonly Random random = new Random();

        public static long GetRandomGrainId()
        {
            return safeRandom.Next();
        }
    }
}