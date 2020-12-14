clean:
	gradle clean

package:
	gradle clean build -x test

publish:
	gradle clean deploy

release:
	git fetch --all
	git checkout master
	git pull --ff-only
	python3 update_release_version.py
	VERSION=$$(cat VERSION); \
	git add -u; \
	git commit -m "Update versions for release"; \
	git checkout -b release_$$VERSION; \
	git tag -a $$VERSION -m "Release $$VERSION"; \
	git checkout master; \
	python3 prepare_next_development.py $$VERSION; \
	git add -u; \
	git commit -m "Update for next development version [skip ci]"; \
	git push origin master; \
	git push origin release_$$VERSION \
		-o merge_request.create \
		-o merge_request.target=release \
		-o merge_request.remove_source_branch \
		-o merge_request.title="Merge release version $$VERSION"; \
	git push --tags; \
	git branch -d release_$$VERSION

tests:
	gradle clean test

.PHONY: clean package publish release tests
