def getResolvedOptions(argv, options):
    args = {}
    for i, arg in enumerate(argv):
        for opt in options:
            if arg == f"--{opt}" and i + 1 < len(argv):
                args[opt] = argv[i + 1]
    for opt in options:
        if opt not in args:
            args[opt] = ""
    return args
