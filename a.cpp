class Solution
{
public:
    int findDepth(TreeNode *root, TreeNode *node)
    {
        if (root == node)
            return 0;

        if (root == NULL)
            return -1;

        int foundLeft = findDepth(root->left, node);
        if (foundLeft != -1)
            return 1 + foundLeft;

        int foundRight = findDepth(root->right, node);
        if (foundRight != -1)
            return 1 + foundRight;

        return -1;
    }

    TreeNode *lowestCommonAncestor(TreeNode *root, TreeNode *p, TreeNode *q)
    {
    }
};