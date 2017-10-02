# Contributing Guide

## Developer

1. Follow the **Getting Started** instructions from [README.md](README.md)
2. Make sure you have the latest code

    ```
    git pull
    ```

3. Create a development branch using the *GitHub issue number* and a brief description as the name (e.g., `issues/12345-fix-ssh-problem`)

    ```
    git checkout -b <branch name> master
    ```

4. Make changes using your favorite editor
5. Add changes to git

    ```
    git add <new/modified/deleted files>
    ```

6. Commit the changes using a short summary as part of the commit message header

    ```
    git commit -m "<summary of the changes>"
    ```

7. Push your development branch to this repository. If you are not team member then push this to your forked repository.

    ```
    git push -u origin <branch name>
    ```

8. Open up a pull request (see [GitHub Help](https://help.github.com/articles/creating-a-pull-request/)) and assign it to at least one team member
9. Wait for review comments and address them by repeating steps 4-7. Note that the pull request will be automatically updated with your changes
10. Once the pull request has been approved merge the pull request (see [GitHub Help](https://help.github.com/articles/merging-a-pull-request/))
11. Delete the merged branch
12. Remove your local development branch and the remote tracking one

    ```
    git branch -D <branch name>
    git remote prune origin
    ```

## Reviewer

> Be diligent and try to maintain a good middle ground between being too picky and starting larger design discussions

1. Check changes for
  * intent - do the changes perform the functionality described
  * design - are the changes easy to understand and there is a clear flow of things
  * documentation - is there enough module, class, function, segment documentation to understand why each change is made
2. Make sure those suggestions are addressed
3. Approve the changes
