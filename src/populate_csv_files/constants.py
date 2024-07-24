from src.utils import root_directory

# TODO 2024-02-08: add authors from articles and research papers to authors and firms
# TODO 2024-02-08: add all alt-L1s C-level: sol, aptos, sei, sui, monad [...]
# TODO 2024-02-08: add all L2s C-level across ETH and BTC
AUTHORS = ['Uri Klarman', 'Robert Miller', 'Tarun Chitra', 'Hasu', 'Dan Robinson', 'Jon Charbonneau', 'Tim Roughgarden', 'Scott Kominers', 'Dan Morehead', 'Joey Krug', 'Fenbushi Capital',
'Barnabe Monnot', 'Guillermo Angeris', 'Stephane Gosselin', 'Mallesh Pai', 'daian', 'Alex Nezlobin', 'Jason Milionis', 'Drew Van der Werff', '0xmaki',
'Max Resnick', 'Quintus Kilbourn', 'Georgios Konstantopoulos', 'Alex Obadia', 'Su Zhu', 'Vitalik Buterin', 'Justin Drake', 'Mike Neuder']

FIRMS = ['eigenlayer', 'dymension', 'bloxroute', 'aptos', 'monad', 'sui', 'sei', 'panoptic', 'sol', 'solana', 'circle', 'chainlink', 'pyth', 'raydium', 'jito', 'orca', 'jupiter',
         'cosmos', 'stargate', 'layerzero', "injective", 'cowswap', 'astaria',
         'aave', 'compound', 'dydx', 'curve', 'maven', 'a16z', 'celestia', 'flashbots', 'paradigm', 'smg', 'rig', 'balancer', 'sushiswap', 'uma', 'across', 'primev', 'magic eden',
         'uniswap', 'altlayer', 'aztec', 'starkware', 'scroll', 'optimism', 'zksync', 'matter labs', '1inch', 'anoma', 'espresso', 'blast', 'blast l2', 'ambient', 'ambient finance']


# Note, the use of keywords List is an attempt at filtering YouTube videos by name content to reduce noise
KEYWORDS_TO_INCLUDE = ['perps', 'derivatives', 'options', 'order flow', 'orderflow', 'transaction', 'mev', 'ordering', 'sgx', 'intent', 'dex', 'front-running', 'arbitrage', 'back-running',
            'maximal extractable value', 'trading games', 'timing games', 'arbitrage games', 'timing', 'on-chain games', 'pepc', 'proposer', 'builder', 'mevblocker', 'zeromev', 'libmev',
            'mevboost.pics', 'mev-aware', 'fees', 'pbs', '4337', 'account abstraction', 'boost', 'defi', 'uniswap', 'hook', 'hyperchain', 'erc404', 'erc 404', 'solidity & foundry', 'course', 'invariant test',
            'suave', 'gas war', 'modular summit', 'latency', 'market design', 'searcher', 'staking', 'pre-merge', 'post-merge', 'code audit', 'audit code', 'upgradable smart contract',
            'liquid staking', 'crediblecommitments', 'tee', 'market microstructure', 'rollups', 'rollup', 'roll-up', 'roll-ups', 'uniswap', 'appchain', 'telegram bot', 'L3',
            'cow', 'censorship', 'liquidity', 'censorship', 'ofa', 'pfof', 'payment for order flow', 'decentralisation', 'decentralization', "incentive", "incentives", 'hyperliquid',
            'erc', 'eip', 'auction', 'mechanism design', 'Price-of-Anarchy', 'protocol economics', 'pools', 'censorship', 'starknet', 'nftfi', 'dencun', 'the chopping block',
            '1559', 'BFT', 'selfish mining', 'vickrey auctions', "How They Solved Ethereum's Critical Flaw", 'cryptoeconomics', 'fuzzing', 'formal verification', 'First-Price Auctions',
            'token design', 'token economics', 'crypto economics', "John Adler : Wait, It's All Resource Pricing?", 'evm', 'whiteboard series']

KEYWORDS_TO_INCLUDE += AUTHORS
KEYWORDS_TO_INCLUDE += FIRMS

# , 'smart contract', 'eth global',  'evm',  #  'vitalik', 'buterin', bridge',
KEYWORDS_TO_EXCLUDE = ['sanction','howey','lawyer','joke', 'jokes', '#short', '#shorts', 'gensler', 'T-Shirt', "New year's breathing exercise",
                       'From lifespan to healthspan (1)', 'On promoting healthspan and quality of life (2)',
                       'Quick Bits', '#eth', 'Oslo Freedom Forum:', 'Why the SEC', 'SEC Commissioner', 'Oráculos', 'On promoting healthspan and quality of life',
                       'From lifespan to healthspan', 'On the decentralized web', 'Web3 Masterclass for JavaScript Developers',
                        'Preprofessional Course in Civil Engineering', 'Professional Courses in APAM',
                        'Professional Courses in Biomedical Engineering', 'Professional Courses in Chemical Engineering',
                        'Pre-Professional Course in Earth and Environmental Engineering', 'Tristan Naumann - Computer Science',
                       'The SEC favors cash over in-kind transactions when it comes to approving a spot Bitcoin ETF', 'Art and Awe in the Age of Machine Intelligence',
                       "The Builder-Hero's Journey", 'OgleCrypto tells the fascinating story of how he tracked down a group of DeFi hackers from Hong Kong',
                       "How DeFi Hack Negotiators Get the Job Done: The Chopping Block", "🧐 The proposed IRS reporting rules could adversely impact DeFi.",
                       'The new proposed IRS rules for reporting on crypto transactions are “unadministrable”', 'Oráculos', 'Web3 Masterclass for JavaScript Developers',
                       'The SEC favors cash over in-kind transactions when it comes to approving a spot Bitcoin ETF', "The Builder-Hero's Journey", 'OgleCrypto',
                       'How DeFi Hack Negotiators', 'IRS', 'LINK to Staking v0.2', 'Querying and Indexing Smart Contract Data on Ethereum', 'Recapitalizing the Degens',
                       'finance “shittier” than the ones in crypto.', 'Elisa Konofagou']

YOUTUBE_VIDEOS_CSV_FILE_PATH = f"{root_directory()}/data/links/youtube/youtube_videos.csv"
