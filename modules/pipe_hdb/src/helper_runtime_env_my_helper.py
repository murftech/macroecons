
# ######## ENVIRONMENT STUB PENDING DRY GIT #############
# # ── WHICH ENVIRONMENT? (at most one is True; the real 3 are mutually exclusive) ──
# IS_DATABRICKS    = os.path.isdir('/databricks') or 'DATABRICKS_RUNTIME_VERSION' in os.environ # use two becasue number 2 may be required on classic clusters
# IS_IPYTHON       = ('IPython' in sys.modules) and not IS_DATABRICKS # not databricks is unfortunately needed because it pulls and ipython in
# IS_SH            = ('__file__' in globals())  # `python x.py`: has a real script file on disk
# IS_LOCAL = IS_IPYTHON or IS_SH

# tester
# IS_DATABRICKS=True
print(f'[ENVIRONMENT check] IS_SH: {IS_SH},  IS_IPYTHON: {IS_IPYTHON}, IS_DATABRICKS: {IS_DATABRICKS}')
n_env = sum([IS_SH, IS_IPYTHON, IS_DATABRICKS])
if n_env > 1:
    raise Exception('More than 1 runtime turned up true. Edge case is happening. Script cannot continue. Debug in dev')
elif n_env == 0:
    raise Exception('No runtime could be identified with our conditionals. Script might not continue correctly. Debug in dev')
else:
    print('okay to proceed')


# '''
# IS_DATABRICKS
# # In databricks, sys.argv[0] := '/Workspace/Users/murftech7@gmail.com/deployments/pipe_hdb/src/xxx.py'
# IS_SH:
# # if we use python xxx.py, __file__ is always the path ending with xxx.py, resolve to absolute, then up one space onto it's src
# # for now, xxx.py and everything it imports (providers/, helper_datagov.py, helper_transit.py, …) must be flat siblings in the same directory.
# IS_IPYTHON:
# Ruled that ipython should always be turned on from repo root.
# '''

# # helper
# if IS_DATABRICKS:
#     _src = Path(sys.argv[0]).resolve().parent
# elif IS_SH:
#     _src = Path(__file__).resolve().parent
# elif IS_IPYTHON:
#     _src = Path.cwd() / 'modules' / 'pipe_hdb' / 'src'
# print(_src)
# sys.path.insert(0, str(_src))
# ######## ENVIRONMENT STUB PENDING DRY GIT #############

