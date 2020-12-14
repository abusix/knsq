from sys import argv

release_version = argv[-1]

main = int(release_version.split(".")[0])
feature = int(release_version.split(".")[1])
fix = int(release_version.split(".")[2])

version = "%s.%s.%s-SNAPSHOT" % (main, feature, fix + 1)

with open("VERSION", "w") as f:
    f.write(version + "\n")
