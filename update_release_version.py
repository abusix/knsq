import re

with open("VERSION", "r") as f:
    dev_version = f.read().strip()

if re.fullmatch(r"\d+\.\d+\.\d+-SNAPSHOT", dev_version) is None:
    print("Current VERSION does not match the format X.Y.Z-SNAPSHOT")
    exit(1)

version = dev_version[:-9]

user_input_version = input("Please specify release version [%s]: " % version)

if user_input_version.strip():
    if re.fullmatch(r"\d+\.\d+\.\d+", user_input_version.strip()) is None:
        print("Specified version string does not match the format X.Y.Z")
        exit(1)

    main_dev = int(version.split(".")[0])
    feature_dev = int(version.split(".")[1])
    fix_dev = int(version.split(".")[2])

    main = int(user_input_version.split(".")[0])
    feature = int(user_input_version.split(".")[1])
    fix = int(user_input_version.split(".")[2])

    if main < main_dev \
            or (main == main_dev and feature < feature_dev) \
            or (main == main_dev and feature == feature_dev and fix < fix_dev):
        resp = input(("Specified version %s is lower than current development version %s. \n"
                      % (user_input_version, version)) + "Do you really want to continue? [y/N]: ")
        if resp.strip().lower() != "y":
            exit(1)

    version = user_input_version

with open("VERSION", "w") as f:
    f.write(version + "\n")
