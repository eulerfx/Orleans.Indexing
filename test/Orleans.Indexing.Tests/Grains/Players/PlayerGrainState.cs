using System;

namespace Orleans.Indexing.Tests
{
    [Serializable]
    [GenerateSerializer]
    public class PlayerGrainState : IPlayerProperties
    {
        public string Email { get; set; }
        public int Score { get; set; }
        public string Location { get; set; }
    }
}
