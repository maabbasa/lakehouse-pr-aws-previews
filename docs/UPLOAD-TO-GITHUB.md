# Upload this AWS revision with GitHub Desktop on macOS

1. Unzip the package. Keep the folder structure intact.
2. In GitHub Desktop, select your existing `lakehouse-pr-previews` repository and choose **Repository → Show in Finder**.
3. Copy the extracted package's contents into that repository folder. Replace matching files. In Finder press **Command + Shift + .** to show hidden files, and copy the included `.github` directory as well. Keep the repository's `.git` directory intact.
4. Return to GitHub Desktop. Confirm the changes include `aws/template.yaml`, `aws/job.py`, `aws/preview_core.py`, `aws/run.py`, and both AWS workflow files.
5. Commit with the summary `Add AWS EMR output previews`, then click **Push origin**.
6. Open the repository's **Actions** tab. The validation workflow runs without AWS access. Read `aws/README.md` before deploying or enabling the AWS workflow.

The authoring connection could not access the private repository, so this ZIP has not already been uploaded. No AWS credentials belong in the files or GitHub repository.
