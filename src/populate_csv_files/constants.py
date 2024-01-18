from src.utils import root_directory

# Note, the use of keywords List is an attempt at filtering YouTube videos by name content to reduce noise
KEYWORDS_TO_INCLUDE = ['order flow', 'orderflow', 'transaction', 'mev', 'ordering', 'sgx', 'intent', 'dex', 'front-running', 'arbitrage', 'back-running',
            'maximal extractable value', 'trading games', 'timing games', 'arbitrage games', 'timing', 'on-chain games', 'pepc', 'proposer', 'builder', 'barnabe',
            'fees', 'pbs', '4337', 'account abstraction', 'boost', 'defi', 'uniswap', 'hook', 'anoma', 'espresso',
            'suave', 'flashbots', 'celestia', 'gas war', 'hasu', 'dan robinson', 'jon charbonneau', 'robert miller', 'paradigm',
            'altlayer', 'tarun', 'modular summit', 'latency', 'market design', 'searcher', 'staking', 'pre-merge', 'post-merge',
            'liquid staking', 'crediblecommitments', 'tee', 'market microstructure', 'rollups', 'rollup', 'roll-up', 'roll-ups', 'uniswap', '1inch', 'appchain',
            'cow', 'censorship', 'liquidity', 'censorship', 'ofa', 'pfof', 'payment for order flow', 'decentralisation', 'decentralization', "incentive", "incentives",
            'erc', 'eip', 'auction', 'daian', 'mechanism design', 'Price-of-Anarchy', 'protocol economics', 'stephane gosselin', 'su zhu', 'pools', 'censorship',
            '1559', 'BFT', 'selfish mining', 'vickrey auctions', 'Alex Nezlobin', 'Jason Milionis', "How They Solved Ethereum's Critical Flaw", 'cryptoeconomics',
            'token design', 'token economics', 'crypto economics', 'scroll', 'starkware', 'aztec', 'zksync', "John Adler : Wait, It's All Resource Pricing?"]

# , 'smart contract', 'eth global',  'evm',  #  'vitalik', 'buterin', bridge',
KEYWORDS_TO_EXCLUDE = ['joke', 'jokes', '#short', '#shorts', 'gensler', 'T-Shirt', "New year's breathing exercise",
                       'From lifespan to healthspan (1)', 'On promoting healthspan and quality of life (2)',
                       'Quick Bits', '#eth', 'Oslo Freedom Forum:', 'Why the SEC', 'SEC Commissioner', 'Oráculos', 'On promoting healthspan and quality of life',
                       'From lifespan to healthspan', 'On the decentralized web', 'Web3 Masterclass for JavaScript Developers',
                       'The SEC favors cash over in-kind transactions when it comes to approving a spot Bitcoin ETF', 'Art and Awe in the Age of Machine Intelligence',
                       "The Builder-Hero's Journey", 'OgleCrypto tells the fascinating story of how he tracked down a group of DeFi hackers from Hong Kong',
                       "How DeFi Hack Negotiators Get the Job Done: The Chopping Block", "🧐 The proposed IRS reporting rules could adversely impact DeFi.",
                       'The new proposed IRS rules for reporting on crypto transactions are “unadministrable”']

YOUTUBE_VIDEOS_CSV_FILE_PATH = f"{root_directory()}/data/links/youtube/youtube_videos.csv"
